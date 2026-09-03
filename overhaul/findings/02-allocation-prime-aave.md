Status: DRAFT — investigation in progress

# 02 — Prime / allocation / Aave-like lending stack

Area owner: investigation agent 02. Finding ids `F02.x`.
Evidence gathered read-only at `main` @ `c4e0a8f2`; `go build ./...` and `go vet` over the area are clean.

---

## 1. Area map

Three pipelines share a neighbourhood, a domain vocabulary (prime, star, ALM proxy,
allocation, position, reserve, sweep, snapshot) and almost no code.

**A. Allocation pipeline** (`prime-allocation-indexer` → `allocation_tracker`) is the mature one.
SQS block event → read receipts from Redis → `TransferExtractor` matches ERC-20 Transfers against
the ALM proxies → matched `TokenEntry`s route through a `SourceRegistry` to one of five real
`PositionSource` adapters (`balanceof`, `erc4626`, `erc7540`, `curve`, `uni-v3`) plus skip/stub
placeholders → each source multicalls on-chain state **pinned to the block hash** → the
`PrimePositionHandler` resolves token metadata, applies a per-token-type denomination policy, and
writes `allocation_position` + `token_total_supply` in one transaction. Entries, proxies, stars and
per-chain scoping all come from a vendored `axis_synome_contract` JSON.

**B. Aave-like pipeline** (`sparklend-indexer` → `aavelike_position_tracker` → `pkg/aavelike`,
plus the `aave-like-user-snapshot-indexer` backfiller). Event-driven: decode Supply/Borrow/
Repay/Withdraw/Liquidation/CollateralToggle logs, then re-read the user's whole reserve set from
`UiPoolDataProvider` and persist positions, events, borrower records and reserve snapshots across
eight repositories.

**C. Reference-capital pipeline** (`reference-capital-indexer` cronjob + `reference-capital-backfill`)
pulls prime-level risk-capital and balance-sheet figures from upstream HTTP monitors and writes
four registry-FK'd tables. `prime_debt` (`prime-debt-indexer`) reads Sky/Maker Vat debt per prime.

```
                  axis-synome JSON (vendored)
                            │
  SQS block ──► allocation_tracker.Service ──► SourceRegistry ──► 5 PositionSource adapters
                            │                                       │ outbound.Multicaller
                            └──► PrimePositionHandler ──► AllocationRepository
                                        │                 TokenTotalSupplyRepository
                                        └── metadataCache (its own multicall)

  SQS block ──► aavelike_position_tracker.Service ──► pkg/aavelike.PositionReader
                    │  (8 repos, *ethclient.Client)        └─► BlockchainService (ABI per version)
                    └──► shared by cmd/backfillers/aave-like-user-snapshot-indexer

  Temporal  ──► reference_capital_indexer  ─┐
  one-shot  ──► reference_capital_backfill ─┴─► 4 provider ports + 4 repository ports
  SQS block ──► prime_debt.VaultDebtService ──► outbound.VatCaller  (the area's deepest port)
```

Wiring is entirely per-binary: each of the six `main.go` files hand-rolls config parsing, pool
setup, ABI loading, telemetry and lifecycle. There is no shared worker bootstrap in this area,
though one exists (`cmd/workers/internal/dexbootstrap`) and is used by one unrelated worker.

---

## 2. Metrics

### Size

| Package / binary | src LOC | test LOC | src files | test files |
|---|---|---|---|---|
| `internal/services/allocation_tracker` | 3522 | 7551 | 18 | 24 |
| `internal/services/aavelike_position_tracker` | 1569 | 2532 | **2** | 2 |
| `internal/pkg/aavelike` | 1603 | 2263 | 4 | 2 |
| `internal/services/reference_capital_indexer` | 677 | 1118 | 2 | 2 |
| `internal/services/prime_debt` | 382 | 1121 | 1 | 1 |
| `internal/services/sparklend_backfill` | 300 | 604 | 1 | 1 |
| `internal/services/reference_capital_backfill` | 162 | 229 | 1 | 1 |
| `internal/services/sparklend` | **34** | **0** | 1 | 0 |
| `cmd/backfillers/aave-like-user-snapshot-indexer` | 461 | 288 | 1 | 1 |
| `cmd/workers/prime-allocation-indexer` | 385 | 935 | 1 | 2 |
| `cmd/workers/sparklend-indexer` | 337 | 397 | 1 | 2 |
| `cmd/workers/prime-debt-indexer` | 285 | 938 | 1 | 2 |
| `cmd/backfillers/sparklend-backfill` | 274 | 971 | 1 | 2 |
| `cmd/backfillers/reference-capital-backfill` | 273 | **0** | 2 | 0 |
| `cmd/cronjobs/reference-capital-indexer` | 150 | **0** | 1 | 0 |
| **Total** | **10 414** | **18 947** | 38 | 44 |

Plus `postgres/allocation_repository.go` 315, `token_total_supply_repository.go` 187,
`position_repository.go` 186, `entity/allocation_position.go` 137.

### Largest files

`aavelike_position_tracker/service.go` 1161 · `pkg/aavelike/blockchain_service.go` 1124 ·
`allocation_tracker/handler_prime_positions.go` 584 · `pkg/aavelike/position_reader.go` 337 ·
`allocation_tracker/service.go` 433 · `reference_capital_indexer/service.go` 540 ·
`allocation_tracker/chains.go` 203 (mostly comment).

### Largest functions (34 functions in the area exceed 60 lines)

| lines | location | function |
|---|---|---|
| 268 | `cmd/backfillers/aave-like-user-snapshot-indexer/main.go:185-452` | `run` |
| 217 | `cmd/workers/prime-allocation-indexer/main.go:169-385` | `run` |
| 177 | `cmd/workers/sparklend-indexer/main.go:161-337` | `run` |
| 172 | `cmd/backfillers/sparklend-backfill/main.go:103-274` | `run` |
| 134 | `aavelike_position_tracker/service.go:928-1061` | `PersistUserPositionBatch` |
| 130 | `cmd/workers/prime-debt-indexer/main.go:156-285` | `run` |
| 125 | `allocation_tracker/source_balanceof.go:71-195` | `FetchBalances` |
| 123 | `aavelike_position_tracker/service.go:410-532` | `saveReserveDataSnapshot` |
| 113 | `pkg/aavelike/blockchain_service.go:590-702` | `BatchGetTokenMetadata` |
| 111 | `aavelike_position_tracker/service.go:682-792` | `savePositionSnapshot` |
| 110 | `pkg/aavelike/position_reader.go:171-280` | `GetBatchUserPositionData` |
| 102 | `prime_debt/service.go:271-372` | `syncAll` |
| 99 / 87 / 76 | `prime-allocation-indexer:69-167`, `sparklend-indexer:73-159`, `prime-debt-indexer:77-152` | three `parseConfig` |

The three worker `parseConfig` functions total **262 lines**; the three worker `run` functions
total **524 lines**.

### Ports

| port | non-test files referencing it |
|---|---|
`AllocationRepository`, `TokenTotalSupplyRepository`, `PositionRepository`, `PrimeDebtRepository`, `VatCaller`, `PrimeCapitalStackRepository`, `PrimeCapitalStackAllocationRepository`, `PrimeReferencePositionRepository`, `ReferencePositionProvider`, `RiskCapitalProvider`, `RiskCapitalAllocationProvider` | **2 each** (one adapter + one caller) |
| `PrimeBalanceSheetRepository`, `BalanceSheetProvider` | 3 each |

11 of 13 area ports have exactly one adapter and one caller — hypothetical seams by the brief's
definition. `aavelike_position_tracker` alone consumes **8** repository ports through a 13-argument
constructor (`service.go:76-90`).

### Test doubles

28 hand-rolled doubles in the area, despite `internal/testutil/` already shipping
`mock_multicaller.go`, `mock_sqs_consumer.go`, `mock_block_cache.go`, `mock_tx_manager.go`,
`mock_token_repository.go`, `mock_protocol_repository.go`, `mock_user_repository.go`,
`mock_event_repository.go`, `mock_receipt_token_repository.go`, `mock_debt_token_repository.go`.

| package | doubles | notes |
|---|---|---|
| `reference_capital_indexer` | 10 | imports no `testutil`; `fakeTxManager` (`service_test.go:60`) duplicates `testutil/mock_tx_manager.go` |
| `allocation_tracker` | 8 | `mockHandler` (`log_handler_test.go:14`) and `testHandler` (`service_test.go:27`) both double the same in-package `AllocationHandler` |
| `prime_debt` | 4 | `fakeSQSConsumer` (`service_test.go:43`) duplicates `testutil/mock_sqs_consumer.go` |
| `reference_capital_backfill` | 3 | `mockPrimeRepo` / `mockSheetRepo` are same-named twins of `reference_capital_indexer`'s |
| `sparklend_backfill` | 2 | |
| `aavelike_position_tracker` | 1 | `mockPositionRepository` (`service_test.go:1034`) — only 1 double for 8 ports, because `*ethclient.Client` cannot be faked (see F02.4) |
| `pkg/aavelike` | 0 | |

Repo-wide, `outbound.Multicaller` is hand-doubled in 7 test files, `outbound.SQSConsumer` in 7,
`outbound.BlockCacheReader` in 5.

### Repeated convention counts

| convention | occurrences |
|---|---|
| `!result.Success \|\| len(result.ReturnData) == 0` multicall guard | 8 in the area, **30** repo-wide (non-test) |
| `AllowFailure: true` call sites | 21 in the area |
| `failures = append(failures, …)` accumulate-then-`strings.Join` | 10 in three `source_*.go` files |
| `TokenMetadata` / `tokenMeta` value type defined locally | **4** (`pkg/aavelike:29`, `morpho_indexer:73`, `fluid_vault_indexer:30`, `allocation_tracker/handler_prime_positions.go:403`) |
| `metadataCache map[common.Address]TokenMetadata` | 3 |
| `Pack("decimals")` / `Pack("symbol")` sites | 8 non-test files |
| `blocksSinceSweep` / `SweepEveryNBlocks` sweep counter | 3 services, 3 divergent semantics |
| star→prime-id resolution | 3 divergent conventions |
| `unpackUint256` helper | 2 definitions, different signatures |
| `TransactionReceipt` struct | 3 definitions |

---

## 3. Findings

### F02.1 — The position-shape pipeline is four parallel structs and five hand-copy sites; `types.go` has never changed without the handler changing

**Strength**: Strong
**Size**: L (2–4 PRs)

**Files**
- `internal/services/allocation_tracker/types.go:44-66` (`PositionBalance`), `:94-122` (`PositionSnapshot`), `:71-74` (`PoolSupply`), `:127-136` (`TokenTotalSupplySnapshot`)
- `internal/services/allocation_tracker/service.go:324-344` (copy 1), `:400-414` (copy 2), `:368-377` (copy 3)
- `internal/services/allocation_tracker/handler_prime_positions.go:207-227` (copy 4), `:381-398` (copy 5)
- `internal/domain/entity/allocation_position.go:17-47`

**Problem**
One position travels through four near-identical structs, hand-copied field by field at five
sites. `PositionBalance` has six fields, **four of which are documented as "set only by source X"**:

```go
UnderlyingValue *big.Int        // erc4626 + uni_v3 only          types.go:47-54
PoolToken0/1    *common.Address // set only by UniV3Source         types.go:56-59
ShareToken      *common.Address // set only by ERC7540Source       types.go:60-65
```

`PositionSnapshot` re-declares the same four (`types.go:98-105`), and both copy sites in
`service.go` list them again. So the *interface* of `PositionSource` grows every time an adapter
is added — the definition of a shallow seam.

Git confirms the cost precisely: **all 8 commits since March that touch `types.go` also touch
`handler_prime_positions.go`** (8/8 = 100%; 7/8 also touch `service.go`). `types.go` has never
moved alone. The co-change knot `service.go` ↔ `handler_prime_positions.go` is 8 shared commits
out of 14 and 12 respectively (Jaccard 44%), and `allocation_repository.go` co-changes with the
handler 6/11. The two widest commits in the area are exactly this shape:
`4dd59ed8` "Index and utilize total supply" — 46 files, reaching six of seven `source_*.go` files,
the handler, the service, the types, a new repo + port and 18 Python files; and `52623743`
"underlying_value + underlying_token_id" — 19 files.

**Proposed change**
Cut the seam where the knowledge is, not where the read is. Let a source return an already-interpreted
position, and delete the handler's re-dispatch:

```go
type PositionSource interface {
    Name() string
    Supports(tokenType, protocol string) bool
    // Observe reads state at blockHash and returns positions this source has
    // already interpreted: it owns its own denomination policy and names the
    // addresses each row's metadata must be read from.
    Observe(ctx context.Context, entries []*TokenEntry, blockHash common.Hash) (*Observation, error)
}

type Position struct {
    Entry    *TokenEntry
    Balance  *big.Int
    Scaled   *big.Int          // nil where the concept does not apply
    Value    *Valuation        // nil + a FailureReason, never a bare nil
    RowMeta  RowMetaSource     // an address to read decimals/symbol from, or a composed symbol
}
```

Then one generic mapper `Position → entity.AllocationPosition` replaces copies 1, 2 and 4, and
`handler_prime_positions.go:74-102`, `:151-175`, `:232-287`, `:289-344` (~165 lines that exist
only to switch on `TokenType`) collapse into the sources that own each policy.

**Benefits**
Adding a source becomes 2 files (new `source_x.go` + one `Register` line) instead of the 8–12
non-test files a new *field* costs today. `types.go`'s 100% co-change with the handler drops to
zero because the union type disappears. Each source's denomination policy becomes testable
against that source's own test file instead of through `handler_prime_positions_test.go`
(currently 45 KB, the largest test file in the area).

**Risk / migration**
Land in three steps: (1) introduce `Position`/`Valuation` and a generic mapper, keeping
`PositionBalance` as an adapter shim; (2) move the `underlyingValuation` switch arms into their
sources one arm per PR — the switch at `handler_prime_positions.go:296-328` has four arms, each
independently movable; (3) delete the shim and the union fields. The guardrail tests
(`routing_guardrail_test.go`, `handler_prime_positions_test.go`) pin the observable behaviour
throughout. Risk is concentrated in the `uni_v3` arm, the only one that composes a symbol.

**Depends on / enables** Enables F02.2, F02.6.

---

### F02.2 — The multicall build/execute/decode/collect-failures skeleton is written out ~10 times

**Strength**: Strong
**Size**: M (one PR, < 1000 lines)

**Files**
- `allocation_tracker/source_curve.go:87-138` — build, `ExecuteAtHash`, guard, unpack, type-assert, `failures`, `strings.Join`
- `allocation_tracker/source_erc4626.go:181-221`, `:244-263`, `:270-281`
- `allocation_tracker/source_erc7540.go:108-175`, `:197-229`
- `allocation_tracker/source_balanceof.go:71-195` (125-line `FetchBalances` with a hand-rolled `callKind`/`callContext` parallel-slice tagging scheme, `:55-69`)
- `allocation_tracker/handler_prime_positions.go:436-518`, `:521-571`
- `pkg/aavelike/blockchain_service.go:590-702` (`BatchGetTokenMetadata`, 113 lines)

**Problem**
Every one of these repeats the same five-step ritual: pack per target, append
`outbound.Call{Target, AllowFailure: true, CallData}`, execute, walk results checking
`!mc[i].Success || len(mc[i].ReturnData) == 0`, unpack, type-assert, then accumulate a
`[]string` of failures and escalate with `strings.Join`. The guard appears **30 times**
repo-wide (8 in this area); `AllowFailure: true` appears **21 times** in this area; the
accumulate-then-join idiom appears **10 times** across three source files.

The ritual is not merely verbose — it is where the AGENTS.md never-swallow rule is enforced, and
the copies do not agree:

- `source_erc4626.go:265-269` documents the divergence in a comment: *"The aToken source's
  warn-and-drop is an older deliberate deviation this source does not copy."* Compare
  `source_erc4626.go:270-281` (a failed `totalSupply` returns an error) with
  `source_balanceof.go:144-192` (a failed `totalSupply` logs a Warn and drops the supply row).
- `source_curve.go:90-97` and `source_balanceof.go:206-224` silently `continue` past an ABI
  **pack** failure, so a broken ABI can shrink a batch to zero entries with no error.
  `source_curve.go:106-108` then returns `(nil, nil, nil)` and `FetchBalances` reports an empty
  success. `source_erc4626.go:186-189` and `source_erc7540.go:200-203` treat the same pack
  failure as a hard error.
- `unpackUint256` exists twice with different contracts:
  `source_balanceof.go:258-268` returns `nil` on failure; `blockchain/psm3_caller.go:466`
  returns an `error`.

**Proposed change**
One typed helper in `internal/pkg/blockchain`, deep enough to own the invariant:

```go
// Batch collects calls with a decoder per call and executes them pinned to a
// block hash. A sub-call that fails or fails to decode is an error naming every
// failed target — never a dropped row.
type Batch struct{ … }
func (b *Batch) Add(target common.Address, abi *abi.ABI, method string, args ...any) *Slot
func (b *Batch) Execute(ctx context.Context, mc outbound.Multicaller, at BlockRef) error
func (s *Slot) Uint256() (*big.Int, error)
func (s *Slot) Address() (common.Address, error)
func (s *Slot) Optional() bool   // explicit, structural opt-out — not a swallowed failure
```

`Slot` removes the parallel-slice indexing (`source_balanceof.go`'s `callKind`/`callContext`,
`source_erc4626.go`'s `supplyStart` offset arithmetic at `:198,:212,:216`) which is the actual
source of the off-by-one risk in these files.

**Benefits**
The never-swallow rule is enforced once, at a seam, instead of at ~30 call sites by convention.
`source_balanceof.go:71-195` (125 lines) and `BatchGetTokenMetadata` (113 lines) both drop under
the one-screen threshold. The `Optional()` opt-out makes AGENTS.md's *"gate it structurally"*
rule mechanical rather than reviewer-enforced.

**Risk / migration**
Additive: land the helper with tests, convert one source per PR. `source_curve.go` first (52
lines, the smallest and the one with the pack-failure bug), then `erc7540`, `erc4626`,
`balanceof`, then the two `metadataCache` decoders, then `pkg/aavelike`. Each conversion is
behaviour-preserving except where it deliberately turns a current warn-and-drop into an error —
call those out per PR.

**Depends on / enables** Enables F02.3, F02.4.

---

### F02.3 — Token metadata (decimals/symbol) is implemented four times, with four different caches and two different symbol-decoding policies

**Strength**: Strong
**Size**: M

**Files**
- `allocation_tracker/handler_prime_positions.go:403-406` (`tokenMeta`), `:408-427` (`metadataCache`), `:436-518` (`fetchMissing`), `:521-571` (`decodeTokenMeta`/`decodeDecimals`/`decodeSymbol`), `:575-584` (`decodeBytes32Symbol`)
- `pkg/aavelike/blockchain_service.go:29-33` (`TokenMetadata`), `:77` (`metadataCache`), `:590-702` (`BatchGetTokenMetadata`)
- `services/morpho_indexer/blockchain_service.go:73` (`TokenMetadata`), `:88`, `:1148-1185`, `:1251-1330`, `:1375-1377`
- `services/fluid_vault_indexer/blockchain_service.go:30` (`TokenMetadata`)
- also packing decimals/symbol: `pkg/blockchain/feed_prices.go`, `pkg/blockchain/erc4626_share_prices.go`, `morpho_indexer/vault_probe.go`, `cmd/backfillers/morpho-vault-backfill/prober.go`

**Problem**
"Read an ERC-20's decimals and symbol, cache it per address" is the single most duplicated
behaviour in the area. Four packages declare the value type; three declare a
`map[common.Address]TokenMetadata` cache; eight non-test files pack the calls.

The copies have diverged on a genuinely hard question — MKR-class tokens returning `bytes32`
instead of `string`. `allocation_tracker` handles it with a structural shape check and a
documented rationale (`handler_prime_positions.go:548-584`: an ABI string is ≥ 64 bytes, so an
exactly-32-byte payload is unambiguously `bytes32`). Nothing in the grep shows
`pkg/aavelike`'s `BatchGetTokenMetadata` carrying the same fallback. Two of the area's writers
therefore disagree about whether an MKR-class symbol is readable, and both write to the same
`token` registry table, which AGENTS.md declares the single FK source for
address/symbol/decimals.

`allocation_tracker`'s cache also has a subtlety the others cannot share because it is not
extracted: it is deliberately number-pinned rather than hash-pinned
(`handler_prime_positions.go:484-486` — decimals/symbol are structurally immutable, so VEC-471
does not apply). That reasoning is correct and belongs in one place.

**Proposed change**
`internal/pkg/blockchain/tokenmeta`: one `TokenMeta{Symbol, Decimals}` value type, one
`Cache` with `Resolve(ctx, addrs []common.Address, at BlockRef) (map[common.Address]TokenMeta, error)`,
the `bytes32` fallback and the number-pinning rationale stated once. Have the four sites depend on
it; delete the four local types and three local caches.

**Benefits**
The `bytes32` decision, the never-cache-a-fallback rule
(`handler_prime_positions.go:499-504` — a cached fallback would survive every SQS redelivery
until restart) and the pinning choice each get one canonical site, matching AGENTS.md's
"state each why once". Every consumer inherits the fix rather than only the package that hit
the bug.

**Risk / migration**
Start with the two writers to `token` (`allocation_tracker`, `pkg/aavelike`) since they are the
correctness-relevant pair; `morpho_indexer` and `fluid_vault_indexer` follow as separate PRs
(cross-area — coordinate with the owners of those areas). Low risk: the behaviour is a pure
function of the on-chain reads, and `handler_prime_positions_test.go` already covers the
`bytes32` and never-cache-a-fallback paths.

**Depends on / enables** Depends on F02.2 (uses the batch helper). Cross-area.

---

### F02.4 — `pkg/aavelike` and `aavelike_position_tracker` inject a concrete `*ethclient.Client`; they are the only two such packages, and the repo already has the fix pattern three times over

**Strength**: Strong
**Size**: M

**Files**
- `pkg/aavelike/blockchain_service.go:65`, `:120`; `pkg/aavelike/position_reader.go:21`, `:31`
- `services/aavelike_position_tracker/service.go:57`, `:80`, `:1118`
- The established pattern, for contrast: `pkg/blockchain/blocktime/cache.go:20`
  (*"HeaderFetcher is the subset of `*ethclient.Client` the cache needs, narrowed so…"*),
  `services/morpho_v2_bootstrap/bootstrap.go:77` (*"`*ethclient.Client` satisfies it"*),
  `services/oracle_backfill/service.go:27` (*"Satisfied by `*ethclient.Client`"*)

**Problem**
AGENTS.md is explicit: define an interface in `ports/outbound`, never import infrastructure into
application code. A grep for `ethclient.Client` across `internal/services` and `internal/pkg`
returns 8 non-test files. Five are legitimate — the dialer (`rpchttp/retry_transport.go:412`),
the multicall adapter (`blockchain/multicall/client.go`), and the three sites above that narrow it
to a named interface and say so in a comment. The remaining three are `pkg/aavelike` (×2) and
`aavelike_position_tracker` (×1), which take the concrete type.

The consequence is measurable, not theoretical. `aavelike_position_tracker` consumes 8 repository
ports and hand-rolls exactly **one** test double (`mockPositionRepository`,
`service_test.go:1034`) — because the on-chain read path cannot be faked at all, its
`service_test.go` is **84 KB** (the largest file in the area, 12 commits since March) and the
package needs a bespoke `testutil/sparklend_mock_rpc.go` to test anything. `pkg/aavelike` has
**zero** doubles and 2263 lines of test.

`callContractState` (`blockchain_service.go:222-227`) is the leak in miniature: it exists only
because `*ethclient.Client` offers `CallContract` and `CallContractAtHash` as separate methods,
so the service must branch on which one to use.

**Proposed change**
Add one narrow port and let the concrete client satisfy it, exactly as the three sibling packages
already do:

```go
// ports/outbound: StateCaller performs a single pinned eth_call.
// *ethclient.Client satisfies it.
type StateCaller interface {
    CallContract(ctx context.Context, msg ethereum.CallMsg, block *big.Int) ([]byte, error)
    CallContractAtHash(ctx context.Context, msg ethereum.CallMsg, hash common.Hash) ([]byte, error)
}
```

Better still, fold it into a domain-shaped port the way `prime_debt` does (F02.7) so the service
never sees `ethereum.CallMsg` at all.

**Benefits**
`aavelike_position_tracker`'s eight save-paths become unit-testable with a fake caller instead of
a mock RPC server, which is what would let the 1161-line `service.go` be split at all (F02.6).
Restores the dependency direction AGENTS.md declares and the rest of the repo follows.

**Risk / migration**
Mechanical and low risk: change three type declarations, three constructor signatures, add the
port. Nothing else changes. One PR.

**Depends on / enables** Enables F02.6.

---

### F02.5 — `reference_capital_indexer` and `reference_capital_backfill` duplicate their shared logic three times over, and the duplicate has a latent star-resolution bug

**Strength**: Strong
**Size**: S (one PR, < 300 lines)

**Files**
- `reference_capital_indexer/service.go:389-419` (`toBalanceSheets`) vs `reference_capital_backfill/service.go:132-162` (`toSnapshots`) — 31 lines, line-for-line
- `reference_capital_indexer/service.go:473-487` vs `reference_capital_backfill/service.go:116-130` (`primeIDsByName`) — 15 lines, verbatim
- `reference_capital_indexer/service.go:447-471` (`reportBalanceSheetUncovered` + `uncoveredTrackedStars`) vs `reference_capital_backfill/service.go:97-114` (`requireEveryStarCovered`)
- `reference_capital_indexer/service.go:385-387` (`normalizedStar`) vs `reference_capital_backfill/service.go:100`, `:105`, `:127` (inlined three times)

**Problem**
Two services build the same `entity.PrimeBalanceSheetSnapshot` from the same
`outbound.BalanceSheetDay` and write it to the same table. The two functions differ in three
ways: the function name, the error wording, and — the one that matters — **the backfill omits
the star normalization on lookup**:

```go
// indexer  service.go:395
primeID, ok := primeIDs[normalizedStar(day.Star)]
// backfill service.go:138
primeID, ok := primeIDs[day.Star]
```

The backfill's own `primeIDsByName` (`:127`) builds that map with
`strings.ToLower(strings.TrimSpace(p.Name))` keys. So any upstream star carrying uppercase or
padding resolves fine in the cronjob and fails the backfill with
`"upstream feed reported unknown prime"`. The two code paths cannot agree by construction, and
the backfill is the one an operator reaches for when the series has a hole.

`normalizedStar` is a two-line function (`:385-387`) that the sibling inlines three times rather
than importing — the clearest possible signal that these two services want to be one.

**Proposed change**
Extract the shared half into one place. Either a small internal package
(`internal/services/referencecapital/` holding `NormalizeStar`, `PrimeIDsByStar`,
`BalanceSheetSnapshots`, `UncoveredStars`), or better: move star resolution behind the
repository as `PrimeRepository.IDsByStar(ctx) (map[string]int64, error)` so normalization
happens once, next to the registry it resolves against, and neither service can get it wrong.
Keep the two *policies* distinct and explicit — `requireEveryStarCovered` (a seed must be
complete) vs `reportBalanceSheetUncovered` (an incremental cycle may lag) is a real difference
worth naming — but compute "which tracked stars are missing" once.

**Benefits**
Fixes the bug by construction. Removes ~60 duplicated lines and, with them, 3 of
`reference_capital_backfill`'s doubles (`mockPrimeRepo`, `mockSheetRepo`, `mockProvider` are
same-purpose twins of the indexer's). AGENTS.md's *"FK by natural key only (`prime`: `name`)"*
gains a single enforcement point.

**Risk / migration**
One PR. Add a test that a mixed-case star resolves identically through both paths — it fails
today. `reference-capital-backfill` and `reference-capital-indexer` have **zero test files** at
their `main.go` level, so add the integration test AGENTS.md requires while there.

---

### F02.6 — `aavelike_position_tracker/service.go` is a 1161-line god file holding nine responsibilities behind one 13-argument constructor

**Strength**: Strong
**Size**: L

**Files**
- `services/aavelike_position_tracker/service.go` (1161 lines, 2 source files in the whole package, 11 commits since March)
- `:53-74` (struct, 8 repo ports + concrete eth client), `:76-132` (13-arg constructor)
- `:928-1061` `PersistUserPositionBatch` (134), `:410-532` `saveReserveDataSnapshot` (123), `:682-792` `savePositionSnapshot` (111), `:584-666` `saveCollateralToggleEvent` (83)
- `pkg/aavelike/blockchain_service.go` (1124 lines), `:590-702` `BatchGetTokenMetadata` (113)

**Problem**
One file carries: the SQS loop, receipt fetch and JSON parse, log routing, six distinct `save*`
paths, token resolution, batch persistence, and borrower records. Four of its functions exceed
100 lines; AGENTS.md names a body longer than one screen an extraction signal and says the
composition rule is *"strongest for orchestration functions (block/event handlers…)"*. The
package has 2 source files and 2 test files, one of which is 84 KB.

The `save*` functions all share the same undeclared shape — begin a transaction, resolve
user/protocol/token FK ids, write an event row, snapshot the position — but each re-derives it
inline with a 7-to-8 parameter signature threading
`(protocolAddress, chainID, blockNumber, blockHash, blockVersion, blockTimestamp, txHash)`
through by hand.

**Proposed change**
Two moves. First, make the per-block context a value instead of seven parameters —
`type BlockRef struct { ChainID int64; Number int64; Hash common.Hash; Version int; Timestamp time.Time }`
— which the whole area would benefit from (`prime_debt/service.go:271`,
`allocation_tracker/service.go:386` and every `source_*.go` thread the same tuple). Second, split
the file along the responsibilities that already have names: `receipts.go` (fetch/parse/route),
`events.go` (the six `save*` paths), `positions.go` (snapshot + batch persist), `reserves.go`
(reserve data). Group the eight repositories into the two or three cohesive units they actually
form (`PositionWriter`, `EventWriter`, `RegistryResolver`), which cuts the constructor from 13
arguments to 5.

**Benefits**
Each `save*` path becomes independently testable, which is the precondition for shrinking the
84 KB test file. `BlockRef` deletes the same 5-tuple from ~20 signatures across four packages
and makes the hash-vs-number pinning question (F02.8) a property of one type.

**Risk / migration**
Do F02.4 first — without a fakeable on-chain read the split cannot be verified cheaply. Then
`BlockRef` as its own mechanical PR, then one file-split PR per responsibility. Highest-risk
step is `PersistUserPositionBatch`, which the backfiller also calls.

**Depends on / enables** Depends on F02.4.

---

### F02.7 — Three levels of on-chain-read abstraction do the same job; the deepest one is used by the smallest service

**Strength**: Worth exploring
**Size**: L

**Files**
- Deepest: `prime_debt/service.go:69` consumes `outbound.VatCaller` — a domain-shaped port with `ResolveIlks` / `ReadDebts(ctx, queries, blockHash)`; the service contains no ABI, no `Call`, no `Result`. Its whole `outbound` surface is 6 symbols.
- Middle: `allocation_tracker` injects `outbound.Multicaller` into each source; the sources own ABI packing and decoding (`source_*.go`, 21 `AllowFailure` sites).
- Shallowest: `pkg/aavelike/blockchain_service.go` holds six `*abi.ABI` fields (`:67-72`), a version switch (`loadABIs`, `:159-211`), a raw byte-offset fallback decoder (`decode.go:44-118`) and a concrete eth client.

**Problem**
Three services in one area read on-chain state three different ways, and the quality ordering is
inverse to the size ordering. `prime_debt` — 382 source lines, the smallest real service here —
has the cleanest seam: `entity.ComputeDebtWad(r.Art, r.Rate)` is pure domain arithmetic over a
port result, and the adapter (`blockchain/vat_caller.go`) owns every ABI concern. Meanwhile
`pkg/aavelike`, at 1603 lines, has no port at all.

There is a second, subtler consequence. `prime_debt`'s `VatCaller` returns per-query results
carrying an explicit `Reverted` flag distinct from `Err` (`service.go:298-323`), which lets the
service state its partial-failure policy in domain terms: a single revert is *"no debt this
block"*, a structural error propagates, and all-reverts is a hard error because *"an all-empty
result is far more likely a misconfiguration than a real coordinated 'no data'"* (`:344-353`).
The `Multicaller` port cannot express that distinction, so every `source_*.go` re-derives it from
`Success` bits and disagrees (see F02.2).

**Proposed change**
Treat `VatCaller` as the reference shape. For the Aave-like stack, define
`outbound.AaveLikeReader` with the reads the domain actually wants —
`UserReserves(ctx, user, at BlockRef)`, `ReserveData(ctx, asset, at BlockRef)` — and move
`pkg/aavelike`'s ABI handling, version switch and raw decoder into
`adapters/outbound/blockchain/aavelike_caller.go` where the psm3 and vat callers already live.
`pkg/aavelike` then has no reason to exist: it is a package under `pkg/` whose entire content is
an unported adapter.

**Benefits**
Answers the brief's question about `pkg/aavelike`'s location: it is in `pkg/` because it is
shared by two consumers (`aavelike_position_tracker/service.go:23` and
`cmd/backfillers/aave-like-user-snapshot-indexer/main.go:28`) — a real seam by fan-in — but
`adapters/outbound/blockchain/` is where a shared adapter belongs, and moving it there makes the
port explicit. Deleting it passes the deletion test in the right direction: the complexity does
not vanish, it relocates to the layer that should hold it.

**Risk / migration**
XL if done wholesale; do it as the last step of F02.4 → F02.6. The version switch and the
Avalanche `bool`-as-`uint256` raw decoder (`decode.go:12-43`) are the load-bearing parts and are
well covered by `blockchain_service_test.go`.

**Depends on / enables** Depends on F02.4, F02.6.

---

### F02.8 — Reorg pinning is enforced by the type in one package and by a silent runtime fallback in the other

**Strength**: Strong
**Size**: S

**Files**
- Structural: `allocation_tracker/types.go:147-155` — `FetchBalances(ctx, entries, blockHash common.Hash)` takes **no** block number, with the invariant in the doc comment; all 6 read sites use `ExecuteAtHash` (`source_curve.go:110`, `source_erc4626.go:149`, `:204`, `source_balanceof.go:82`, `source_erc7540.go:129`, `:207`)
- Conditional: `pkg/blockchain/oracle.go:25-30` (`ExecutePinned`) — `if blockHash != (common.Hash{}) { ExecuteAtHash } else { Execute(number) }`; 5 call sites
- `pkg/aavelike/blockchain_service.go:212-214` (`executeState`) and `:222-227` (`callContractState`) — both take `(blockNumber int64, blockHash common.Hash)` and branch
- Deliberate exception, correctly documented: `handler_prime_positions.go:484-488` (metadata is immutable, so number-pinning is fine)

**Problem**
VEC-471 — after a reorg an archive node answers `eth_call`-by-number with the *new* canonical
state, which can silently disagree with the reorged version the message is for — is the area's
sharpest correctness invariant, stated in `types.go:150-154`, `prime_debt/service.go:209-211`,
`ports/outbound/multicaller.go:12`, `ports/outbound/psm3.go:29` and `ports/outbound/vat_caller.go:24`.

`allocation_tracker` enforces it in the type: a source cannot make a number-pinned read because
it never receives a number. `pkg/aavelike` enforces it by convention: pass a zero hash and every
read silently degrades to number-pinned, with no error and no log. The comment at
`blockchain_service.go:209-211` names the intended users of that fallback
(*"backfill/CLI callers"*) — but nothing in the type distinguishes "I have no hash because I am
replaying history" from "I have a hash and forgot to pass it".

**Proposed change**
Make the two cases distinct types rather than one zero-value check:

```go
type BlockRef struct{ … }              // constructors only:
func At(hash common.Hash) BlockRef     // reorg-correct
func AtNumberUnpinned(n int64, why string) BlockRef  // explicit, for replay; loggable
```

`ExecutePinned` then takes a `BlockRef` and cannot be handed an accidental zero. This composes
with the `BlockRef` proposed in F02.6 — one type carries chain, number, hash, version and
timestamp, and its constructor is where the invariant lives.

**Benefits**
The invariant moves from five doc comments and a runtime branch to one type with two named
constructors. A backfiller's unpinned read becomes visible in code review and in logs instead of
indistinguishable from a bug.

**Risk / migration**
One PR for the type plus the 5 `ExecutePinned` call sites and the 2 `pkg/aavelike` helpers.
Behaviour-preserving; the only change is that a zero hash becomes a compile error rather than a
silent fallback.

**Depends on / enables** Pairs with F02.6.

---

### F02.9 — Six worker/backfiller composition roots hand-roll 1007 lines of bootstrap; the shared helper that fixes it already exists and is used by one worker

**Strength**: Strong
**Size**: L

**Files**
- `cmd/workers/prime-allocation-indexer/main.go` (385 lines; `run` 169-385 = 217; `parseConfig` 69-167 = 99; 18 commits since March — the most-churned `main.go` in the repo)
- `cmd/workers/sparklend-indexer/main.go` (337; `run` 177; `parseConfig` 87; 12 commits)
- `cmd/workers/prime-debt-indexer/main.go` (285; `run` 130; `parseConfig` 76; 12 commits)
- `cmd/backfillers/aave-like-user-snapshot-indexer/main.go` (461; `run` 185-452 = **268 lines**, the largest function in the area; 8 commits)
- `cmd/backfillers/sparklend-backfill/main.go` (274; `run` 172; 7 commits)
- The precedent: `cmd/workers/internal/dexbootstrap/bootstrap.go` (274) + `parseconfig.go` (211), used by `cmd/workers/dex-indexer/main.go` — **118 lines**

**Problem**
The brief asks why `prime-allocation-indexer/main.go` churns. It is not the domain. Of **42
file-touches across the three worker mains since March, zero were driven by a new allocation
source, a new chain, or a new prime.** The classification:

| bucket | file-touches | share |
|---|---|---|
| WIRING (a dependency injected, repo added, telemetry added) | 16 | 38% |
| REFACTOR/RENAME | 12 | 29% |
| CONFIG (env var, flag, timeout) | 8 | 19% |
| BUGFIX RIPPLE (a fix elsewhere forced an edit here) | 6 | 14% |
| NEW SOURCE / NEW CHAIN / NEW PRIME | **0** | **0%** |

81% is WIRING + CONFIG + RIPPLE — the cost of six hand-rolled bootstrap sequences. The repo
already knows this: commit `58f9c196`'s own subject is *"fix worker bootstrap drift across the 7
hand-rolled SQS worker mains"*, and it touched all three of these files. `17b08499` is a
one-line `ethclient.DialContext` → `rpchttp.DialEthereum` swap replicated across all three plus
two backfillers. `b44e0b47` is a one-line pool-config swap, again ×3. `5c8566bd` deleted 46 lines
of hand-rolled shutdown from each and replaced them with `lifecycle.RunWithTimeoutGuard`.

The strongest evidence that this is a real seam and not a hypothetical one:
`internal/pkg/telemetry/otel.go:47` adds a **runtime** assertion
(`assertNoInstrumentsPredateTelemetry`) that no instrument is created before `InitOTEL`, and its
comment explains why it cannot be a lint over `cmd/**/main.go`: *"half the affected binaries open
their pool inside a shared helper (cmd/workers/internal/dexbootstrap), where per-main source
order says nothing."* The repo pays for a runtime guardrail because the ordering invariant cannot
be enforced statically across N hand-rolled mains.

The co-change matrix confirms the three mains are one unit, not three:
`prime-allocation ∩ sparklend` = 11 shared commits (of 18 and 12);
`sparklend ∩ prime-debt` = 10 (of 12 and 12).

**Proposed change**
Generalise `dexbootstrap` into `cmd/workers/internal/workerboot`: `ParseConfig` (the common env
vars and the flag-wins-over-env precedence rule that `58f9c196` had to fix in three places),
`Bootstrap` (pool via `postgres.WorkerDBConfig`, Redis cache, SQS consumer, multicaller,
`rpchttp.DialEthereum`, telemetry in the correct order, archiving wire), `Deps.Close()`, and
`lifecycle.Run`. Each `main.go` keeps only what is genuinely its own: which service to construct
and which repositories it needs. `dex-indexer` at 118 lines is the target shape.

**Benefits**
Turns 16 WIRING + 8 CONFIG + 6 RIPPLE touches into 1 edit each. Makes the telemetry-ordering
assertion enforceable at the seam instead of at runtime. Gives the two composition roots with
**zero test files** (`cmd/backfillers/reference-capital-backfill`,
`cmd/cronjobs/reference-capital-indexer`) a tested bootstrap for free, closing an AGENTS.md gap
(*"main.go entry points should also have 100% coverage"*).

**Risk / migration**
Incremental and low risk: extract `ParseConfig` first (three near-identical 76-99 line
functions), verify with the existing `main_test.go` files, then `Bootstrap` one binary at a time
starting with `prime-debt-indexer` (the smallest). The backfillers join last — their `run`
functions are the longest but they share the least.

**Cross-area**: this affects all 12 `cmd/workers` and most backfillers. It is the single
highest-leverage change touching my area and should be owned repo-wide, not by this area.

---

### F02.10 — Source→token-type routing is a first-match-wins scan with no disjointness guardrail, and the gap has already cost six weeks of wrong data

**Strength**: Strong
**Size**: S

**Files**
- `allocation_tracker/source_registry.go:35-42` (`Route` — linear scan, first match wins)
- `allocation_tracker/registry_build.go:11-19` (*"in the registration order the worker relies on (earlier sources win in Route)"*)
- Six different `Supports` predicate styles: `source_erc4626.go:68`, `source_curve.go:40`, `source_erc7540.go:65`, `source_univ3.go:40`, `source_balanceof.go:52` (map), `source_stubs.go:28-36`, `:66-68`
- `source_balanceof.go:41-44` — the scar tissue
- `served_chains_guardrail_test.go:51` (`TestAcknowledgedSetsAreDisjoint`) — the guardrail that exists for the *chain* partition
- `routing_guardrail_test.go:57` (`TestEveryContractEntryRoutes`) — the guardrail that exists for source routing

**Problem**
Which source owns a token type is decided by a hand-maintained registration order in
`BuildSourceRegistry` and six independently-written `Supports` predicates. `TestEveryContractEntryRoutes`
checks that every entry routes to *something* and not to an unallowlisted stub — but nothing
checks that exactly one source claims each token type.

That hole has already produced wrong data. `source_erc7540.go` was written in `7a9a46df`
(2026-06-11) but not registered until `1e7ea976` (2026-07-24) — **six weeks** during which
`centrifuge` entries kept routing to `BalanceOfSource`, which the code now documents was reading
ERC-7540 *vault* addresses that *"are not tokens and revert on balanceOf/decimals"*
(`source_balanceof.go:41-44`). The routing guardrail passed the whole time, because the entries
did route somewhere. And the registration itself did not land in the source's own PR: it arrived
inside an unrelated 20-file axis-synome contract bump.

The same invariant *is* guarded on the chain axis. `chains.go` maintains a three-way partition
with a dedicated `TestAcknowledgedSetsAreDisjoint`, a `validateContractChainsServed` guardrail and
a boot-time `AssertServedTrackerChain` — 203 lines of machinery for a set of 10 chain names, and
worth it. The source axis has none of it.

**Proposed change**
Replace the implicit ordering with one declarative table and check it:

```go
// One row per token_type. A duplicate key is a compile-time-adjacent failure
// (a map literal cannot hold one) and a missing key fails the routing guardrail.
var sourceForTokenType = map[string]sourceKind{
    "erc20": kindBalanceOf, "atoken": kindBalanceOf, /* … */
    "centrifuge": kindERC7540, "erc4626": kindERC4626,
    "uni_v3_pool": kindUniV3, "uni_v3_lp": kindUniV3,
    "psm3": kindStub, "centrifuge_feeder": kindStub, "galaxy_clo": kindStub,
    "anchorage": kindSkip,
}
```

A Go map literal cannot contain a duplicate key, so the "two sources claim one type" bug becomes
unrepresentable. Add `TestEveryRegisteredSourceIsReachable` (a written source that nothing routes
to fails CI) — which would have caught the six-week gap on day one.

**Benefits**
Deletes the load-bearing-registration-order comment and the `first match wins` semantics.
Brings the source partition up to the standard `chains.go` already sets for chains. Removes the
`placeholderSource` marker interface (`source_stubs.go:43-48`), whose only purpose is to
distinguish a stub from a skip at runtime — the table states it directly.

**Risk / migration**
One PR. `Supports(tokenType, protocol)` keeps its `protocol` argument only for
`SkipSource` (`source_stubs.go:28-36`); check whether any real source needs it — none of the five
do, so the parameter can likely go too.

---

### F02.11 — `internal/services/sparklend` is dead code, and it is the third copy of `TransactionReceipt`

**Strength**: Strong
**Size**: S

**Files**
- `internal/services/sparklend/types.go` (34 lines, the whole package)
- `internal/services/shared/types.go:4`, `:22` (the live copy, used by `aavelike_position_tracker`)
- `internal/services/allocation_tracker/service.go:21-29` (a third, narrower copy)

**Problem**
`internal/services/sparklend` has **zero importers** — no Go file outside the directory
references the package path or the identifier, and it has had **zero commits since 2026-02-26**,
before the investigation window. Its doc comment claims it *"provides shared types for SparkLend
services"*; the SparkLend service (`aavelike_position_tracker`) imports
`internal/services/shared` instead. The package is a leftover from
`internal/services/sparklend_position_tracker`, deleted in `c89214b2` (2026-03-18).

Separately, `TransactionReceipt` is declared three times. `allocation_tracker/service.go:21-29`
declares a 7-field version using `[]types.Log` from go-ethereum;
`shared/types.go:4` declares a 13-field version with a local `Log`. Both parse the same cached
receipts JSON from Redis. Two shapes for one wire format is a drift waiting to happen.

**Proposed change**
Delete `internal/services/sparklend`. Then fold `allocation_tracker`'s `TransactionReceipt` into
`internal/services/shared` (or, better, move receipt parsing behind the
`outbound.BlockCacheReader` port so no service parses the JSON at all — the cache adapter is the
only place that should know the wire shape).

**Benefits**
34 lines and one package gone; one receipt shape instead of three. The port-level version would
delete the `json.Unmarshal` from `allocation_tracker/service.go:197-200` and
`aavelike_position_tracker/service.go:196-231`, which are the same six lines twice.

**Risk / migration**
The deletion is zero-risk (verify with `go build ./...`). The receipt consolidation is one PR.

---

### F02.12 — `AllocationRepository` and `TokenTotalSupplyRepository` are near-copies with divergent mutation semantics

**Strength**: Worth exploring
**Size**: S

**Files**
- `postgres/allocation_repository.go:36-53` vs `postgres/token_total_supply_repository.go:32-49` — identical 5-argument constructors, identical bodies
- `allocation_repository.go:58-154` (`SavePositions`) vs `token_total_supply_repository.go:51-110` (`SaveSupplies`) — same skeleton: validate loop → sort by natural key → resolve token FKs → `pgx.Batch` → `SendBatch` → `Exec` loop → `Close`
- `allocation_repository.go:260-315` vs `token_total_supply_repository.go:157-187` — `resolveTokenIDs`, 27 lines duplicated
- `allocation_repository.go:31-34` — `tokenCacheKey`, declared here and used by its sibling

**Problem**
Two repositories written to in the same transaction by the same handler
(`handler_prime_positions.go:124-136`) share four structures and diverge on one that matters:

```go
// allocation_repository.go:78 — sorts the CALLER's slice in place
slices.SortFunc(positions, func(a, b *entity.AllocationPosition) int { … })
// token_total_supply_repository.go:113 — clones first
sorted := slices.Clone(supplies)
```

`SavePositions` reorders a slice the handler still holds (built at
`handler_prime_positions.go:156-229`); its sibling deliberately does not. Both comments cite the
same reason for sorting (stable advisory-lock acquisition order, ADR-0002 §3) — 17 repository
files in `postgres/` reference that convention — but only one of the two avoids the side effect.

**Proposed change**
Extract the shared skeleton as a generic helper — `saveVersionedRows[T]` taking a validator, a
natural-key comparator, an FK resolver and an insert builder — or at minimum make both clone
before sorting and share one `resolveTokenIDs`. The FK resolution is the more valuable extraction:
it is the enforcement point for AGENTS.md's *"FK by natural key only"* and the `LEAST(existing, new)`
`created_at_block` merge, whose rationale is currently written out three times
(`allocation_repository.go:290-293`, `handler_prime_positions.go:182-188`, `:392-396`).

**Benefits**
Removes the in-place-sort side effect. States the `created_at_block` floor convention once.
Any new append-only snapshot repository inherits the advisory-lock ordering instead of
re-deriving it.

**Risk / migration**
Small and self-contained, but `postgres/` is a shared area — coordinate with that area's agent.
The in-place-sort fix is independently landable in a few lines.

**Cross-area**: `postgres/` adapters.

---

### F02.13 — Three services re-implement the periodic-sweep counter with three different semantics

**Strength**: Worth exploring
**Size**: S

**Files**
- `allocation_tracker/service.go:45`, `:64-65`, `:243-257`
- `prime_debt/service.go:29`, `:75`, `:103-104`, `:169`, `:204-222`
- `psm3/service.go:29`, `:66`, `:98-103`, `:157`, `:187-195` (cross-area)

**Problem**
"Every N blocks, re-read all tracked state" is implemented three times, and no two agree:

| | primed at `Start` | counter reset | on sweep failure |
|---|---|---|---|
| `allocation_tracker:247-257` | no (starts at 0) | after success only | propagate → SQS NACK |
| `prime_debt:169, 204-222` | yes (`= N-1`, first block sweeps) | after success only | propagate → SQS NACK |
| `psm3:157, 187-195` | yes (`= N-1`) | **before** the sweep (`:191`) | **log + ACK** |

`allocation_tracker/service.go:243-246` states the invariant explicitly: *"a sweep failure must
NOT reset the counter (so the next block retries the sweep) and must propagate so SQS
redelivers"*. `psm3:191` resets first and then ACKs a failed sweep, with its own multi-line
justification (block events are a FIFO cadence clock, not a unit of work). Both arguments are
coherent; the problem is that the choice is invisible unless you read three services, and each
package proves its own behaviour with its own tests.

**Proposed change**
One `sweepgate` helper alongside `sqsutil.RunLoop`, with the failure policy as an explicit
named option rather than an emergent property of statement order:

```go
gate := sweepgate.New(n, sweepgate.RetryOnFailure)   // or sweepgate.SkipOnFailure
if gate.Due() { if err := sweep(...); err != nil { return gate.Failed(err) }; gate.Done() }
```

**Benefits**
The three-way divergence becomes one word at each call site. The AGENTS.md never-ack-a-partial
rule gets one enforcement point instead of three tested-in-isolation copies.

**Risk / migration**
One PR, mostly mechanical. Decide deliberately whether `allocation_tracker` should also prime its
counter (today it is the only one that does not sweep on the first block after a restart — likely
unintentional).

**Cross-area**: `psm3`.

---

### F02.14 — Hygiene: 34 over-long functions, 6 unpaired test files, 2 untested composition roots, 28 hand-rolled doubles beside a shared mock package

**Strength**: Strong
**Size**: M (or fold into the findings above)

**Files / evidence**
- **34 functions over 60 lines** in the area (list in §2). AGENTS.md names a body longer than one
  screen an extraction signal and records that a `funlen`/`gocognit` linter is the *planned*
  deterministic backstop. Turning it on with a baseline would stop the count growing; four
  functions here exceed 200 lines.
- **6 test files with no source pair** in `allocation_tracker`, which AGENTS.md calls
  *"a smell with exactly two resolutions"*: `loaders_test.go` (pure helpers — belongs in
  `testhelpers_test.go` per the stated exception), `entries_chain_test.go` (tests
  `EntriesAndProxiesForChainID`, which lives in `entries.go` — belongs in `entries_test.go`),
  `guardrails_test.go`, `routing_guardrail_test.go`, `served_chains_guardrail_test.go`
  (arguably the scenario exception, but then they should be named for the scenario), and
  `main_integration_test.go`.
- `entries_chain_test.go:12-26+` chains three independent scenarios in one `TestEntriesAndProxiesForChainID`,
  against AGENTS.md's *"Never chain independent scenarios in one function"*.
- **2 composition roots with zero test files**: `cmd/backfillers/reference-capital-backfill`
  (273 lines across `main.go` + `backfill.go`) and `cmd/cronjobs/reference-capital-indexer`
  (150 lines), against *"`main.go` entry points should also have 100% coverage"*.
- **28 hand-rolled doubles** while `internal/testutil/` already ships mocks for
  `Multicaller`, `SQSConsumer`, `BlockCache`, `TxManager`, `TokenRepository`, `ProtocolRepository`,
  `UserRepository`, `EventRepository`, `ReceiptTokenRepository`, `DebtTokenRepository`.
  `reference_capital_indexer` (10 doubles) imports no `testutil` at all;
  `prime_debt/service_test.go:43` hand-rolls `fakeSQSConsumer` next to
  `testutil/mock_sqs_consumer.go`; `reference_capital_indexer/service_test.go:60` hand-rolls
  `fakeTxManager` next to `testutil/mock_tx_manager.go`.
- `allocation_tracker` doubles its own in-package `AllocationHandler` twice
  (`log_handler_test.go:14` `mockHandler`, `service_test.go:27` `testHandler`).

**Proposed change**
Enable `funlen`/`gocognit` in `.golangci.yml` with a baseline at today's worst so the count can
only fall. Move the four unpaired unit-test files to their pairs. Switch the doubles that have a
`testutil` equivalent. Add the two missing `main_integration_test.go` files (F02.5 touches both
those binaries anyway).

**Benefits**
The composition rule stops depending on a reviewer noticing — AGENTS.md itself records that this
is how *"a 254-line function once slipped through"*, and this area currently holds a 268-line one.

**Risk / migration**
Independent of every other finding; each bullet is its own small PR.

---

## 4. Cross-area observations

- `internal/pkg/telemetry/otel.go:47` pays for a **runtime** assertion that telemetry is
  initialised before any instrument, explicitly because the invariant cannot be linted across N
  hand-rolled `main.go` files. Strong evidence for a repo-wide worker bootstrap (F02.9); affects
  all 12 `cmd/workers`.
- `cmd/workers/internal/dexbootstrap` (485 lines incl. `ParseConfig`) is used by exactly one of
  12 workers. The seam exists and is proven; it is simply unadopted.
- **18 of 45 files in `internal/ports/outbound/` import `github.com/jackc/pgx/v5`**, so `pgx.Tx`
  is threaded through the ports and into services: `allocation_tracker/handler_prime_positions.go:14`,
  `aavelike_position_tracker/service.go`, `reference_capital_indexer/service.go` all import the
  driver directly. `domain/entity` is clean.
- `entity.BlockQuerier` (`domain/entity/debt_types.go:11`) is the **only** interface in
  `domain/entity` — an outbound port living in the domain layer. Consumed by `prime_debt`.
- 17 files in `adapters/outbound/postgres/` reference the ADR-0002 advisory-lock ordering
  convention; it is enforced by a hand-written comparator per repository (F02.12).
- `TokenMetadata` is declared in `morpho_indexer/blockchain_service.go:73` and
  `fluid_vault_indexer/blockchain_service.go:30` as well — those areas share F02.3's problem.
- `psm3/service.go` shares the sweep-counter pattern and is the outlier on failure policy (F02.13).
- The four `reference_capital` provider ports are the same shape
  (`Fetch…(ctx, stars, …) ([]Row, error)` over decimal strings keyed by `Star`) and repeat the same
  *"parsing belongs to the consumer, not the transport"* rationale three times
  (`balance_sheet_provider.go:10`, `risk_capital_provider.go:14`, `:52`). Worth one shared
  `UpstreamFeed` shape if the offchain-feed area agrees.
- Adding a **chain** costs 1 Go line in `chains.go` and 12–17 YAML/Python files
  (`281eab68`: 24 files, 17 of them k8s/alerts/docs; `9f678630`: 17 files, 15 k8s). The Go side is
  a clean seam — `chains.go` has **zero** co-change with `service.go`, `handler_prime_positions.go`,
  `types.go` or `allocation_repository.go`. The fan-out is entirely a k8s-overlay problem and
  belongs to the k8s area.
- Adding a **prime/proxy/receipt-token** now costs 1–3 Go lines
  (`entries.go` / `created_at_blocks.go` / `config.go`) plus a migration plus 3–7 Python files.
  `a834a5fb` "Add AUSDT to allocation tracker" is 1 file, 1 line. The Go side of the entity
  registry is healthy; the Python-side duplication is another area's finding.

---

## 5. Open questions

1. **Is `pkg/aavelike`'s missing `bytes32` symbol fallback a real gap or handled upstream?**
   `allocation_tracker` has an explicit MKR-class fallback (`handler_prime_positions.go:548-584`);
   I found no equivalent in `BatchGetTokenMetadata`. Both write to the `token` registry. Whether
   any SparkLend/Aave reserve actually returns a `bytes32` symbol needs a chain check I cannot do
   read-only.
2. **Was `allocation_tracker` not priming its sweep counter a deliberate choice?**
   `prime_debt:169` and `psm3:157` both set `= N-1` with the comment *"first block triggers
   immediate read"*; `allocation_tracker` does not, so it waits 75 blocks after a restart. No
   comment explains the difference.
3. **Does `Supports`'s `protocol` argument still earn its place?** Only `SkipSource`
   (`source_stubs.go:28-36`) reads it; all five real sources ignore it. Was it for a
   protocol-specific override that has since gone away?
4. **Is `psm3`'s log-and-ACK sweep policy the intended target state?** It is the only one of the
   three that contradicts the invariant `allocation_tracker/service.go:243-246` states as
   VEC-188. If it is right, the other two should follow; if not, it is a live data-hole risk.
5. **Who owns the `entity.ReferenceDataSource` write path?** Both `reference_capital_indexer` and
   `reference_capital_backfill` write `PrimeBalanceSheetSnapshot` with that source tag. Is the
   backfill meant to be idempotent against the cronjob's rows, or seed-only?
6. **How much of the 18 947 test lines is covering behaviour vs. covering the absence of seams?**
   `aavelike_position_tracker/service_test.go` (84 KB, 1 double for 8 ports) and
   `handler_prime_positions_test.go` (45 KB) both look like the shape of tests written against an
   untestable dependency. Confirming that would strengthen F02.4 and F02.6 considerably.
