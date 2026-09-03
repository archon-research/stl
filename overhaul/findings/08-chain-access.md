Status: FINAL

# 08 — Chain access (services ↔ chain RPC)

## 1. Area map

Everything between an indexer and an Ethereum node. Two independent paths:

**Block-payload path** (watcher only): `alchemy.Subscriber` (WebSocket `eth_newHeads`) →
`live_data` → `alchemy.Client` (HTTP JSON-RPC, `outbound.BlockchainClient`) → Redis + SNS.
Workers never touch it; they read the payload from cache.

**State-read path** (every indexer): `rpchttp.DialEthereum` → `*ethclient.Client` →
`multicall.Client` (Multicall3 `aggregate3`) or `multicall.DirectCaller` (batched `eth_call`,
for `msg.sender == 0` contracts) → optionally wrapped by `archiving.Multicaller` → callers.
`outbound.Multicaller` is the only real seam here (3 production adapters, 2 entry points:
`Execute(…, *big.Int)` and `ExecuteAtHash(…, common.Hash)`).

```
BlockEvent{ChainID,BlockNumber,Version,BlockHash,BlockTimestamp}
   │  destructured at the handler (11 × event.ParsedBlockHash())
   ▼
(chainID, blockNumber, blockHash, blockVersion, blockTimestamp)  ← loose tuple
   │  re-passed through 26 signatures that carry hash+version+timestamp adjacently
   ▼  (82 non-test funcs take `blockHash common.Hash`)
call site decides pinning:  ExecuteAtHash (29×)  |  Execute (24×)
   ▼
archiving.Multicaller ── needs number+version back → recovers them from context.Value
   ▼
multicall.Client / DirectCaller → node
```

Supporting packages: `abis` (30 hand-written ABI-JSON getters), `erc20meta` (bytes32/string
decode), `rpcerr` (revert vs transport), `blocktime` (hash→timestamp memo), `retry`,
`rpchttp` (retry transport + `DialEthereum`), `rpcutil`, `hexutil`, `proxytls`, `chainutil`,
`axis_synome_contract` (contract registry loader over `contracts/axis-synome/*.json`).
`adapters/outbound/blockchain` holds two port adapters (`PSM3Caller`, `VatCaller`).
`adapters/outbound/blockverifier` routes `data_validator` to Etherscan.
`internal/testutil/mockchain` is a deployed mock node (k8s dev-infra), not a test double.

## 2. Metrics

| package | files | src | test |
|---|---|---|---|
| adapters/outbound/alchemy | 12 | 1,624 | 4,726 |
| adapters/outbound/blockchain | 4 | 674 | 855 |
| adapters/outbound/blockverifier | 2 | 86 | 45 |
| pkg/blockchain (top level) | 19 | 1,459 | 2,471 |
| pkg/blockchain/abis | 25 | 2,359 | 204 |
| pkg/blockchain/archiving (+archivingwire) | 10 | 667 | 1,202 |
| pkg/blockchain/blocktime | 2 | 49 | 66 |
| pkg/blockchain/erc20meta | 2 | 68 | 102 |
| pkg/blockchain/multicall | 6 | 375 | 872 |
| pkg/blockchain/rpcerr | 2 | 105 | 119 |
| pkg/rpchttp | 4 | 452 | 901 |
| pkg/retry | 2 | 145 | 280 |
| pkg/chainutil | 2 | 108 | 256 |
| pkg/axis_synome_contract | 3 | 237 | 369 |
| pkg/rpcutil / hexutil / proxytls | 6 | 129 | 239 |
| testutil/mockchain | 12 | 1,776 | 2,601 |
| **total** | **113** | **10,313** | **15,308** |

**State reads by pinning** (non-test call sites of the two `Multicaller` entry points; the
`archiving` decorator's own pass-throughs excluded):

| package | by hash | by number | latest |
|---|---|---|---|
| services/morpho_indexer | 11 | 7 | 0 |
| services/allocation_tracker | 6 | 1 | 0 |
| pkg/uniswapv3 | 4 | 0 | 0 |
| services/uniswapv3indexer | 2 | 0 | 0 |
| pkg/blockchain | 2 | 6 | 0 |
| adapters/outbound/blockchain | 2 | 2 | 0 |
| services/shared (`RunSnapshotReads`) | 1 | 0 | 0 |
| **services/fluid_vault_indexer** | **0** | **3** | 0 |
| pkg/aavelike | 0 (1 via `ExecutePinned`) | 1 | 0 |
| cmd/backfillers/morpho-vault-backfill | 0 | 3 | 0 |
| **total** | **28** | **23** | **0** |

Plus 2 direct `eth_call` sites in `pkg/aavelike` (`CallContract` / `CallContractAtHash`
dispatch, `blockchain_service.go:222-227`).

**Signature fan-out of the block-identity tuple** (non-test):
82 funcs take `blockHash common.Hash`; 57 take `blockVersion int`; 29 take
`blockHash, blockVersion` adjacently; **26 take `blockHash, blockVersion, blockTimestamp`
adjacently**; 38 take `outbound.BlockEvent`.

**Repeated skeleton:** 35 multicall result-count checks, 24 `!results[i].Success` checks —
across 17 and 10 files respectively.

**Test doubles:** 14 `ExecuteAtHash` implementations = 3 production + **11 bespoke doubles**
in 8 files + 1 shared `testutil.MockMulticaller` (used by 42 test files).
19 test files stand up an `httptest` fake Ethereum node; only 2 of the 5 that decode
`aggregate3` use the shared `testutil.HandleMulticall3`.
Repo-wide hand-rolled mock/fake/stub types: 155.

**Ports** implemented here: `Multicaller`, `BlockchainClient`, `BlockSubscriber`,
`BlockVerifier`, `PSM3Caller`, `VatCaller`, `CallArchiver` (consumed).

**Largest functions** (non-test, in area):

| lines | location |
|---|---|
| 159 | `pkg/blockchain/abis/vault_v2_events_abi.go:23` `GetVaultV2EventsABI` |
| 142 | `pkg/blockchain/abis/fluid_vault_resolver_abi.go:19` `GetFluidVaultResolverABI` |
| 129 | `adapters/outbound/alchemy/subscriber.go:301` `(*Subscriber).readLoop` |
| 127 | `pkg/blockchain/abis/morpho_blue_abi.go:6` `GetMorphoBlueEventsABI` |
| 99 | `testutil/mockchain/server.go:169` `(*reorgController).trigger` |
| 96 | `pkg/blockchain/erc4626_share_prices.go:116` `ValidateERC4626UnderlyingDecimals` |
| 90 | `adapters/outbound/alchemy/telemetry.go:73` `NewTelemetryWithProviders` |
| 81 | `adapters/outbound/blockchain/vat_caller.go:115` `(*VatCaller).ReadDebts` |
| 74 | `pkg/rpchttp/retry_transport.go:193` `(*retryTransport).RoundTrip` |
| 66/63 | `alchemy/client.go:672` `call` / `:607` `callBatch` (near-duplicate bodies) |

Seven of the ten largest are ABI-JSON string literals wrapped in a function.

---

## 3. Findings

### F08.1 — The pin invariant is enforced by convention at 51 call sites; one live indexer already violates it

**Strength**: Strong
**Files**:
- `internal/ports/outbound/multicaller.go:10-23` (the two entry points and the convention doc)
- `internal/pkg/blockchain/oracle.go:16-30` (`ExecutePinned`, the hash-vs-number dispatch, 5 callers)
- `internal/services/fluid_vault_indexer/blockchain_service.go:145`, `:187`, `:266` (3 number-pinned live reads)
- `internal/services/fluid_vault_indexer/service.go:299` (`processBlockEvent`), `:477` (`snapshotVaults`) — never calls `event.ParsedBlockHash()`
- `internal/services/fluid_vault_indexer/service.go:259`, `:302` — stamps `archiving.WithBlockVersion` but never `WithBlockNumber`
- comparison: `internal/services/curveindexer/stableswap_handler.go:217`, `internal/services/uniswapv3indexer/state.go:147` (hash-pinned via `shared.RunSnapshotReads`)

**Problem.** `Multicaller` exposes two entry points and the choice between them *is* the
reorg-correctness invariant. It is re-decided at each of 51 non-test call sites, documented in
prose at ~14 of them ("Number-pinned intentionally: …", "Hash-pinned so a reorg can't return
another fork's debt"). The only enforcement is a reviewer noticing.

The contrast inside the same interface is the proof: the *other* half of the invariant — never
read at `latest` — **is** enforced by the seam (`multicall/client.go:74-79` rejects a nil
block number, `:106-108` rejects the zero hash; `direct_caller.go:58-60`, `:125-131` mirror
it). Result: **zero** `latest`-pinned state reads in the repo. The half enforced by convention
has a live worker on `main` — `fluid-vault-indexer`, an SQS `BlockEvent` consumer — whose
every state read is number-pinned, and whose archived call records are written with
`blockNumber` recovered from a context value that its handler never stamps (so they key at
block 0, the exact bug `archiving/multicaller.go:125-135` warns about).

Adding a new indexer today means getting six independent things right by hand:
`ParsedBlockHash()` at the handler, `ExecuteAtHash` at every state read, `Execute` at every
static read, `archiving.WithBlockVersion`, `archiving.WithBlockNumber`, and a test double that
proves it (three packages hand-roll that assertion three different ways —
`curveindexer/service_test.go:135` `hashRecordingMulticaller.executedVia`,
`uniswapv3indexer/service_test.go:158` `recordingMulticaller.Execute` returning
`"Execute must not be called; all reads must pin to a block hash"`, and
`testutil.MockMulticaller.Invocations[].ViaHash`).

**Proposed change.** Resolve the pin once, from the event, into a value; derive the reader from
it. Sketch:

```go
// internal/domain/entity (no infrastructure deps)
type Block struct {
    ChainID   int64
    Number    int64
    Hash      common.Hash // zero ⇒ deliberately unpinned (settled replay)
    Version   int
    Timestamp time.Time
}
func BlockFromEvent(e outbound.BlockEvent) (Block, error) // absorbs ParsedBlockHash
func SettledBlock(chainID, number int64, ts time.Time) Block // explicit, named opt-out
func (b Block) Pinned() bool

// internal/ports/outbound
// StateReader is a Multicaller already bound to one block. Pinning, result-count
// checking and archive stamping happen once, here.
type StateReader interface {
    Block() entity.Block
    Read(ctx context.Context, calls []Call) ([]Result, error)
    ReadNamed(ctx context.Context, reads []SnapshotRead) error // today's RunSnapshotReads
}
// StaticProber is the unpinned counterpart, for structurally-static reads
// (token metadata, market params, ilks, PSM3 immutables). Separate type, so
// "this read is number-pinned on purpose" is expressed in the signature.
type StaticProber interface {
    ProbeAt(ctx context.Context, blockNumber int64, calls []Call) ([]Result, error)
}
func PinTo(mc Multicaller, b entity.Block) StateReader
func Prober(mc Multicaller) StaticProber
```

`Multicaller` stays as the transport (and keeps its 3 adapters). Handlers take a
`StateReader`, not a tuple. An unpinned versioned read becomes unrepresentable rather than
merely discouraged.

**Benefits.** *Locality*: hash-vs-number lives in `PinTo`, not 51 call sites; the block number
and version reach the archiver through `r.Block()` instead of two `context.Value` stamps that
one worker already forgets. *Leverage*: 26 signatures shed 3 parameters each; the deepest
chain (`aavelike_position_tracker/service.go:222 → :241 → :260 → :301 → :682 → :858 →
pkg/aavelike/position_reader.go:99 → blockchain_service.go:410 → :212 → ExecutePinned`, 10
hops, peaking at 11 parameters on `snapshotUserPosition:794`) carries one value.
*Tests*: one fake `StateReader` replaces 11 bespoke doubles, and the three hand-rolled
"was it pinned?" assertions become dead — the type system covers them.

**Risk / migration.** Incremental and mechanical. (1) Add `entity.Block` + `PinTo`/`Prober`
with `Multicaller` untouched; (2) convert `services/shared.RunSnapshotReads` to a
`StateReader` method — it already has this shape and 3 callers; (3) convert one indexer per
PR, innermost-first, deleting its tuple parameters as they become unused; (4) fix
`fluid_vault_indexer` as part of its conversion; (5) once every caller holds a `StateReader`,
make `Multicaller` unexported to adapters. Risk is mostly churn: step 3 touches wide
signatures, which is exactly the fan-out being removed. Behaviour changes in exactly one place
(fluid), and that change is a bug fix.

**Size**: XL (epic; ~8 PRs of L/M)
**Enables**: F08.2, F08.3, F08.6

---

### F08.2 — The pack → execute → count-check → success-check → unpack skeleton is repeated ~35× with divergent count checks

**Strength**: Strong
**Files**:
- `internal/services/morpho_indexer/blockchain_service.go:247-265`, `:290-300`, `:355-385`, `:400-430`, `:460-480`, `:505-520`, `:560-580` (6 count checks, 6 success checks)
- `internal/pkg/uniswapv3/reader.go:164`, `:265`, `:332`, `:381` (4 + 4)
- `internal/services/fluid_vault_indexer/blockchain_service.go:145`, `:187`, `:266` (3 + 2)
- `internal/pkg/blockchain/feed_prices.go`, `erc4626_share_prices.go`, `oracle.go`, `resolve_oracle.go`, `curve_lp_prices.go` (11 + 3)
- `internal/adapters/outbound/blockchain/psm3_caller.go:422-447` (`execute`/`executeAtHash`/`checkResultCount` — a local, curried version of the same helper)
- `internal/adapters/outbound/blockchain/vat_caller.go:78-105`, `:139-192` (same package, does *not* use `checkResultCount`; inlines `i >= len(results)` bounds checks instead)
- `internal/pkg/blockchain/rpcerr/rpcerr.go:86-105` (`RequireAllSucceeded`, the shared policy helper — **1 caller**)

**Problem.** Every state read repeats the same five steps. The count check alone appears 35
times in 17 files, in four mutually inconsistent forms: `len(results) == 0`
(`morpho_indexer/blockchain_service.go:261`), `len(results) < 2` (`:371`), `len(results) != N`
(`fluid_vault_indexer/blockchain_service.go:153`), and `i >= len(results)` inside the decode
loop (`vat_caller.go:152`). The first two are strictly weaker than the contract the adapters
already guarantee (one result per call), so they hide a truncation instead of failing on it.
`rpcerr` was written to be the shared policy — its package doc names "DirectCaller, prime_debt,
morpho_indexer, aavelike" as users — but `RequireAllSucceeded` has exactly one caller
(`pkg/aavelike/blockchain_service.go:648`), and the other 24 success checks are hand-rolled.

Two adapters in the *same package* diverge: `psm3_caller.go` has `checkResultCount` and
`unpackAddress`/`unpackUint256` helpers; `vat_caller.go` inlines everything.

**Proposed change.** Fold the count check into the reader from F08.1 (`Read` returns
`len(calls)` results or an error — the check becomes structurally impossible to skip), and make
`ReadNamed` the default for multi-read batches. `services/shared.SnapshotRead` already is that
abstraction (`snapshotread.go:22-68`: pack and decode sit together, offsets tracked centrally)
and it is used by only 3 of the ~35 sites — `curveindexer/{stableswap,cryptoswap}_handler.go`
and `uniswapv3indexer/state.go`. Promote it and route the rest through it. Move
`unpackAddress`/`unpackUint256`/`unpackBool` next to `erc20meta` as a shared `abiunpack`
helper set.

**Benefits.** Locality: one truncation policy, one revert policy. Leverage: each read shrinks
from ~20 lines to a `SnapshotRead` literal, and the positional-cursor class of bug (`results[1+i]`,
`i*2+1`) disappears. Tests: per-read decode is unit-testable without a multicaller at all.

**Risk / migration.** Low; mechanical per call site, no behaviour change except that the two
weak count checks start failing on truncation (correct). Land per package.

**Size**: L
**Depends on**: F08.1 (or can land first, standalone, against `Multicaller`)

---

### F08.3 — Four ERC20-metadata readers, three `TokenMetadata` types, three caches, three failure policies

**Strength**: Strong
**Files**:
- `internal/pkg/aavelike/blockchain_service.go:29-33` (`TokenMetadata{Symbol,Decimals,Name}`), `:589-700` (`BatchGetTokenMetadata`, cache at `:77`)
- `internal/services/morpho_indexer/blockchain_service.go:73-76` (`TokenMetadata{Symbol,Decimals}`), `:1132-1234` (`getTokenMetadata`, `unpackTokenMetadataResults`), `:1236` (`getTokenPairMetadata`), cache at `:88`
- `internal/services/fluid_vault_indexer/blockchain_service.go:30-33` (`TokenMetadata{Symbol,Decimals}`), `:132-180` (`GetTokenMetadata`, no cache)
- `internal/services/allocation_tracker/handler_prime_positions.go:403-406` (`tokenMeta{symbol,decimals}`), `:408` (`metadataCache`), `:436-520` (`fetchMissing`), `:521-575` (own decoders)
- `internal/pkg/blockchain/erc20meta/decode.go:35` (the one genuinely shared piece)

**Problem.** The same read — `symbol()` + `decimals()` (+ `name()`) over a multicaller, cached
in-process, number-pinned — exists four times. They agree on the ABI-decode helper
(`erc20meta.DecodeStringOrBytes32`) and on nothing else:

| | type | cache | symbol decode failure | decimals type |
|---|---|---|---|---|
| aavelike | `TokenMetadata`+Name | yes | hard error, token excluded | `.(uint8)` |
| morpho | `TokenMetadata` | yes | **swallowed**, `Symbol=""` | `intFromAny` |
| fluid | `TokenMetadata` | no | **swallowed**, `Symbol=""` | `.(uint8)` |
| allocation_tracker | `tokenMeta` | yes | hard error, nothing cached | own decoder |

Two of the four write `if sym, err := erc20meta.DecodeStringOrBytes32(...); err == nil { … }`
(`morpho_indexer/blockchain_service.go:1206`, `fluid_vault_indexer/blockchain_service.go:161`),
which is the "swallow a failure into partial success" the root AGENTS.md forbids — an
undecodable symbol persists as `""` and nothing retries it. `aavelike` also drops three Pack
errors on the floor (`blockchain_service.go:610-612`, `decimalsData, _ := s.erc20ABI.Pack(...)`).

**Proposed change.** One `erc20meta.Reader` over the `StaticProber` from F08.1:
`Metadata(ctx, blockNumber, []common.Address) (map[common.Address]entity.TokenMetadata, error)`,
with the cache inside it and one documented policy (decimals and symbol both required; `name`
optional only where the port declares it optional). One `entity.TokenMetadata`. Delete the
three local types and the four fetchers.

**Benefits.** ~250 lines deleted. One place to fix the swallowed-symbol bug. Locality: the
"static metadata is number-pinned on purpose" rationale is stated once (it is currently
copy-pasted as a 4-line comment at `aavelike:585-588`, `morpho:267-271`, `morpho:1239-1241`,
`allocation_tracker:482-484`).

**Risk / migration.** Behaviour change: two indexers start erroring on an undecodable symbol
instead of storing `""`. That is the desired direction but needs a check for existing `''`
symbol rows before flipping. Land the shared reader first with a per-caller policy flag, then
remove the lenient flag.

**Size**: M
**Depends on**: F08.1 for the `StaticProber` (works against raw `Multicaller` otherwise)

---

### F08.4 — 30 ABI getters return `(*abi.ABI, error)` and re-parse JSON on every call, including in a per-block path

**Strength**: Strong
**Files**:
- `internal/pkg/blockchain/abis/*.go` — 30 `GetXxxABI() (*abi.ABI, error)` functions, 2,359 src lines, of which the four largest are single string literals (159/142/127/78 lines)
- `internal/pkg/blockchain/abis/abi_helper.go:9-15` (`ParseABI`, unconditional `abi.JSON`)
- 63 non-test `abis.Get…ABI()` call sites
- `internal/services/uniswapv3indexer/state.go:131-141` — `SnapshotState` (called per pool per block from `service.go:267`) calls `abis.GetERC20ABI()` inline
- `internal/services/uniswapv3indexer/{abi.go:14, state.go:50, tick.go:46, event_decode.go:20}` — four local `sync.OnceValues` caches, working around exactly this
- ABI JSON defined *outside* `abis` and parsed directly: `services/allocation_tracker/source_erc4626.go:54`, `source_erc7540.go:48`, `services/aavelike_position_tracker/event_extractor.go:54`, `:77`, `pkg/uniswapv3/reader.go:72`, `:77`, `testutil/multicall3.go:35`

**Problem.** Three separate costs. (1) Every getter is fallible for a reason that can only be a
malformed compile-time literal, so 63 call sites carry an `if err != nil { return
fmt.Errorf("loading X ABI: %w", err) }` branch that can never fire in a build that passes its
own tests. (2) Nothing caches: `abis.GetERC20ABI()` re-runs `abi.JSON` on a fresh
`strings.Reader` every call, and `uniswapv3indexer.SnapshotState` calls it once per pool per
block. `uniswapv3indexer` noticed and added four `sync.OnceValues` wrappers around its *own*
ABIs — but still calls the uncached `abis.GetERC20ABI()` two lines later. (3) ABI JSON is
authoritative in two places: 30 getters in `abis`, plus 7 literals defined and parsed inside
services, so "where do I add a contract?" has no single answer. `contracts/` holds only the
axis-synome entity registry, not ABIs.

**Proposed change.** Move each ABI to a `.json` file under `contracts/abi/`, load with
`//go:embed`, and expose package-level `var ERC20 = mustParse(erc20JSON)` values parsed once at
init (panicking on a malformed literal is correct — it is a build defect, and `init` is an
entry point under the AGENTS.md panic rule). Signature becomes `abis.ERC20` instead of
`abis.GetERC20ABI() (…, error)`. Pull the 7 in-service literals into the same tree.

**Benefits.** ~63 dead error branches deleted; ~1,200 lines of Go string literal moved to JSON
where an editor can validate it; the per-block re-parse and the four `OnceValues` workarounds
disappear; "add a contract" becomes "add a JSON file and one var". Locality: one ABI tree.

**Risk / migration.** Mechanical and compiler-checked. Keep the old getters as thin
`func GetERC20ABI() (*abi.ABI, error) { return ERC20, nil }` shims for one PR so call sites
migrate independently, then delete. Watch for callers mutating the returned `*abi.ABI` (none
found, but a shared pointer makes that a real hazard — consider returning a value).

**Size**: L

---

### F08.5 — Four retry frameworks; the block-payload path bypasses the one that is repo policy, and retries non-retryable RPC errors

**Strength**: Strong
**Files**:
- `internal/pkg/rpchttp/retry_transport.go:1-23` (policy doc: "`DialEthereum` is the canonical entry-point … so retry protection is automatic rather than opt-in"), `:193-330` (transport, metrics, jitter, `shouldRetry`)
- `internal/pkg/retry/retry.go:15-160` (generic `Do[T]`/`DoVoid`, `Config{MaxRetries, InitialBackoff, MaxBackoff, BackoffFactor, Jitter}`) — 7 non-test callers, none on a chain path
- `internal/pkg/httpclient/client.go:108`, `:184-195` (a third: `retry.DoVoid` + `NonRetryableError`)
- `internal/adapters/outbound/alchemy/client.go:754-800` (`doWithRetry`, a fourth: own loop, own `nonRetryableError` at `:742-750`, `ClientConfig{MaxRetries, InitialBackoff, MaxBackoff, BackoffFactor}` at `:63-105`, **no jitter**, own metrics)
- `internal/adapters/outbound/alchemy/client.go:721-724` — inside the retried closure: `if rpcResp.Error != nil { return fmt.Errorf("RPC error: %s", …) }`
- `internal/adapters/outbound/alchemy/client.go:607-670` vs `:672-740` (`callBatch` / `call`: 63 and 66 lines, near-identical marshal → request → status check → read → unmarshal bodies)
- `internal/adapters/outbound/alchemy/client.go:152-165` (default HTTP client is `otelhttp.NewTransport(http.DefaultTransport)`, not `rpchttp.NewClient`)
- `cmd/base/watcher/main.go:390`, `cmd/workers/raw-data-backup/main.go:268`, `cmd/util/null-payload-refill/main.go:257` (construct `alchemy.Client` with no `HTTPClient`)

**Problem.** The state-read path is consistent: 13 of 13 chain-reading binaries dial through
`rpchttp` (`DialEthereum` ×9, `NewBackfillerClient` ×4), so 429/5xx/network retries, jitter and
`rpc.client.retry` metrics are uniform, and `rpcerr.IsEVMRevert` draws the
revert-vs-transport line once. The block-payload path — the watcher, i.e. the source of every
block event — bypasses all of it and hand-rolls `doWithRetry`.

Two concrete consequences. (a) `call()` retries on `rpcResp.Error != nil`, so a permanent
JSON-RPC application error ("invalid params", an unsupported method on a non-mainnet node)
burns three backoffs (~700 ms) before failing, and the structured `rpc.Error` code is flattened
to a string; `callBatch()` does *not* retry per-response errors, so the same upstream error is
classified two ways within one file. (b) No jitter, so N watcher replicas hitting a 429
re-synchronise.

**Proposed change.** Have `alchemy.NewClient` default its `HTTPClient` to
`rpchttp.NewClient(...)` and delete `doWithRetry`, `nonRetryableError`, and the four backoff
fields from `ClientConfig` (retry becomes a transport concern, as the `rpchttp` package doc
already asserts). Then collapse `call`/`callBatch` into one `post(ctx, payload, out any)`
helper — they differ only in the unmarshal target. Route the "is this error permanent?"
question through `rpcerr` for both paths.

**Benefits.** One retry policy and one metric for both RPC paths; ~130 lines deleted from
`client.go` (806 → ~680); permanent RPC errors fail fast; jitter for free. Tests: the
alchemy client's retry tests move to `rpchttp`, where they already exist.

**Risk / migration.** Timeout semantics differ — `ClientConfig.Timeout` is per-attempt today,
`rpchttp.Config.Timeout` is the whole-request budget including retries. Set
`WithClientTimeout(Timeout × (MaxRetries+1))` on the shim, verify against the watcher's
`e2e` and `client_test.go` (1,232 lines), then simplify. Behaviour change: `GetBlockByNumber`
against a null/erroring upstream fails ~700 ms sooner, which `live_data`'s
`ErrUpstreamNullResult` handling (`rpcutil`) already expects.

**Size**: M

---

### F08.6 — `archiving` smuggles block number and version through `context.Value` because the read carries no block identity

**Strength**: Strong
**Files**:
- `internal/pkg/blockchain/archiving/context.go:7-38` (`WithBlockVersion`, `WithBlockNumber` + getters)
- `internal/pkg/blockchain/archiving/multicaller.go:80-141` — `ExecuteAtHash` passes `nil` for the number; `archiveBatch:94-101` recovers it from the context; `:125-135` warns "archiving hash-pinned SC call batch with no resolvable block number; keying at block 0 … (the VEC-471 bug this fix pays down)"
- 8 stamp sites: `allocation_tracker/service.go:184-185`, `prime_debt/service.go:202-203`, `morpho_indexer/service.go:301-302`, `morpho_indexer/replay.go:61-62`, `:100-101`, `aavelike_position_tracker/service.go:245-246`, `oracle_price_worker/service.go:335-336` — and `fluid_vault_indexer/service.go:259`, `:302`, which stamp only the version
- `internal/ports/outbound/call_archiver.go:26-35` (`CallBatchRecord` needs `ChainID`, `BlockNumber`, `BlockVersion`)

**Problem.** The archiver's record is keyed by `(chainID, blockNumber, blockVersion)`, but
`ExecuteAtHash(ctx, calls, blockHash)` carries none of those. So each of 7 workers must
remember to stamp two context values at the top of its handler, and the decorator recovers them
by dynamic type assertion. `fluid_vault_indexer` stamps one of the two. The failure mode is
silent-ish (a warn log and every batch colliding at block 0), and the code comment records that
this has already happened once.

This is the same missing seam as F08.1 seen from the other side: the pin exists as a
`common.Hash` argument, and everything else about block identity has to travel out-of-band.

**Proposed change.** With `StateReader` from F08.1, `archiving` decorates the reader instead of
the multicaller and builds its record from `r.Block()`. `context.go` and all 8 stamp sites
delete. `archivingwire.Wrap` becomes `func(StateReader) StateReader` (or wraps `PinTo`).

**Benefits.** A whole class of "forgot to stamp" bug removed, plus 38 lines of context plumbing
and 16 call lines. Locality: block identity travels with the read that uses it. Tests: the
archiving tests stop needing to construct contexts.

**Risk / migration.** Low, and it can land as step 2 of F08.1 (the number-pinned `Execute`
path already passes the number positionally, so only the hash path changes).

**Size**: S (given F08.1) / M standalone
**Depends on**: F08.1

---

### F08.7 — `BlockchainClient` is a 13-method port with 7 methods no production code calls

**Strength**: Strong
**Files**:
- `internal/ports/outbound/blockchain_client.go:24-73` (13 methods)
- `internal/adapters/outbound/alchemy/client.go:206-250` (the 7 one-line implementations)
- `internal/testutil/mock_blockchain_client.go:222-316` (their mock counterparts, ~95 lines)

**Problem.** Production callers exist for exactly 6 of 13: `GetBlockByNumber` (4 sites),
`GetBlockByHash` (1), `GetBlockDataByHash` (4), `GetBlocksBatch` (2), `GetCurrentBlockNumber`
(1), plus `GetFinalizedBlockNumber`/`GetBlockHeadersBatch`/`GetBlocksAndReceiptsBatch`/
`GetTracesBatch` which are on the adapter but *not* on the port. The other 7 —
`GetFullBlockByHash`, `GetBlockReceipts`, `GetBlockReceiptsByHash`, `GetBlockTraces`,
`GetBlockTracesByHash`, `GetBlobSidecars`, `GetBlobSidecarsByHash` — have zero non-test callers
and are not used internally either: the batched/parallel paths
(`client.go:261-377`) build their own `jsonRPCRequest` values rather than calling them. Each
appears exactly 3 times in the tree: the port, the adapter, the mock. So the port declares a
by-number/by-hash pair for every data type as if callers chose between them, when in practice
one method (`GetBlockDataByHash`) does all of it.

Meanwhile four methods callers *do* use are missing from the port, so `live_data` and
`backfill_gaps` depend on the concrete `*alchemy.Client` for them — the port is simultaneously
too wide and too narrow.

**Proposed change.** Delete the 7 unused methods from the port, adapter and mock; promote the
four adapter-only methods the services actually call. Result: a ~9-method port that matches its
callers. If the by-hash variants are wanted for a future non-batched path, they can come back
with their first caller.

**Benefits.** ~200 lines deleted across three files; the mock stops being 379 lines; the port
stops implying a choice callers do not make. `AGENTS.md`'s "prefer multiple small interfaces"
is better served by splitting the remainder into `BlockFetcher` / `BatchBlockFetcher` than by
keeping dead methods.

**Risk / migration.** Trivial — compiler-verified. Only the 3 test files exercising them change.

**Size**: S

---

### F08.8 — `alchemy.Subscriber` hand-rolls the WebSocket mechanics that `pkg/wsclient` exists to own

**Strength**: Worth exploring
**Files**:
- `internal/pkg/wsclient/wsclient.go:1-3` ("owns the transport-level mechanics for Gorilla WebSocket clients: dialing, deadlines, pongs, serialized writes, keepalive pings, and clean shutdown"), 316 src lines, `Config` at `:37-45`, `readPump:157`, `pingLoop:176`, `shutdown:216`
- `internal/adapters/outbound/alchemy/subscriber.go:31` (imports `gorilla/websocket` directly), `SubscriberConfig:44-118` (`HandshakeTimeout`, `ReadTimeout`, `PingInterval`, `PongTimeout`, buffer, logger — overlapping `wsclient.Config`), `readLoop:301-430` (129 lines incl. a 55-line inline reader goroutine)
- consumers of `wsclient`: only `adapters/outbound/orderbook/{config,feed}.go`

**Problem.** Two WebSocket connection managers. The one written as the shared abstraction is
used by the CEX orderbook feed; the block subscriber — the single most availability-critical
socket in the system — hand-rolls read deadlines, pong handling, the ping ticker, the reader
goroutine and teardown, and both import `pkg/proxytls` so even the TLS trust is already shared.
`readLoop` is also the largest non-ABI function in the area and reads as a
comment-delimited script (watchdog / reader goroutine / select over 6 cases), which the
function-composition rule in `stl-verify/AGENTS.md` treats as an extraction signal.

The subscriber does own two genuinely additional concerns — reconnect with exponential backoff,
and the VEC-388 data-freshness watchdog (`subscriber.go:309-313`) — but both sit *above* a
connection, not inside one.

**Proposed change.** Reimplement `Subscriber` on `wsclient.Conn`: `Dial` + `Next(ctx)` replace
the reader goroutine, deadlines and ping ticker; the subscriber keeps only subscribe/resubscribe,
the backoff loop, the freshness watchdog and header decoding. Extract `readLoop`'s remaining
body into `forwardHeader` / `handleStall`.

**Benefits.** One socket implementation to reason about under reorg/disconnect; ~150 lines out
of `subscriber.go`; `wsclient` gets a second adapter, making it a real seam rather than a
one-consumer package. Tests: `subscriber_test.go` is 2,301 lines, much of it exercising
transport mechanics that `wsclient_test.go` (782 lines) already covers.

**Risk / migration.** Highest-risk item in this report — this socket feeds every downstream
worker, and the two implementations differ in whether a full inbound buffer drops
(`subscriber.go:406-412` logs "channel full, dropping block" and relies on `backfill_gaps` to
heal) or blocks (`wsclient` buffers 256 and surfaces backpressure). Decide that policy
explicitly before porting. Land behind the existing e2e watcher test; consider keeping it as a
follow-up to F08.1 rather than bundling.

**Size**: L

---

### F08.9 — `internal/testutil/mockchain` (1,776 src lines) is a deployed service living in the test-helper tree, imported by no test

**Strength**: Strong
**Files**:
- `internal/testutil/mockchain/` — 12 files, 1,776 src + 2,601 test lines; `server.go:26-40` (TCP listener, HTTP + WebSocket + admin server), `admin.go`, `replayer.go`, `datastore.go:165` (`LoadFromS3`)
- only importer: `cmd/util/stress-test/mock-blockchain-server/main.go`
- `stl-verify/Dockerfile.mock-blockchain-server`, `Makefile:438-442`, `2658-2680`, `k8s/dev-infra/mock-blockchain-server.yaml`

**Problem.** `mockchain` is a real, containerised, k8s-deployed Ethereum node simulator: it is
built into its own image, pushed to ECR, and is the default chain backend for `make dev-up`
(`AGENTS.md`: "Start kind cluster with full pipeline (mock blockchain server by default)").
Nothing in it is a Go test double — no `_test.go` outside the package imports it, and it has no
`*testing.T` in its API. It sits under `internal/testutil/`, which every other entry in that
directory uses for in-process test helpers, and it is the largest thing there by 4×.

Consequences: it is invisible when looking for the repo's shared chain doubles (which is
`testutil.MockMulticaller`, 42 test files); `internal/testutil` cannot be reasoned about as
"helpers linked into test binaries only"; and its own 2,601 lines of tests run in the unit-test
shard alongside genuine helper tests.

Separately, the actual test-double story for chain access is fragmented: **19** test files stand
up an `httptest` fake node, and only 2 of the 5 that decode `aggregate3` use the shared
`testutil.HandleMulticall3` (`multicall3.go:44`) — `cmd/backfillers/sparklend-backfill`
(13 `aggregate3` references), `cmd/workers/prime-allocation-indexer` (7),
`cmd/workers/morpho-indexer` (4) and `cmd/workers/fluid-vault-indexer` (3) each hand-roll it.

**Proposed change.** Two independent moves. (a) Move `mockchain` to
`internal/mockchain/` (or under `cmd/util/stress-test/mock-blockchain-server/internal/`) and
treat it as the deployable it is — nothing about it changes except the import path and the
mental model. (b) Make one `testutil` fake node the single way an integration test serves
`aggregate3`: extend `testutil.HandleMulticall3` with the per-selector dispatch the four
hand-rolled versions need, and convert them.

**Benefits.** `internal/testutil` becomes scannable; the shared chain double becomes findable;
(b) removes ~200 lines of duplicated calldata parsing from four `main_integration_test.go`
files and gives the F08.1 migration one place to teach about hash pinning.

**Risk / migration.** (a) is a rename plus Dockerfile/Makefile path edits — verify
`make kind-build-mock-blockchain-server` and the `dev-infra` manifest. (b) is per-test and
low-risk.

**Size**: S (a) + M (b)

---

### F08.10 — `blockverifier` is 86 lines of routing for a one-to-one mapping

**Strength**: Worth exploring
**Files**:
- `internal/adapters/outbound/blockverifier/factory.go:17-86` — one `kind` constant, a
  `chainKind` map whose 6 entries all map to `kindEtherscan`, a 2-arm switch whose second arm is
  documented unreachable
- one adapter: `internal/adapters/outbound/etherscan/client.go:26`
- one consumer: `internal/services/data_validator/service.go:57`

**Problem.** Textbook hypothetical seam: one port, one adapter, one caller, one kind. Apply the
deletion test — delete the package and `data_validator`'s composition root calls
`etherscan.NewClient` with a supported-chain check. Complexity does not reappear across N
callers because there is one. The package doc argues for the future ("Adding a future chain
(including a non-EVM one) is a registry entry plus, if needed, a new adapter"), but the
`outbound.BlockVerifier` port already provides that seam; the factory only adds the
chain→kind indirection, and today the chain allowlist is the only part earning its keep.

**Proposed change.** Keep `chainKind` as a plain `supportedChains` set (or fold it into
`entity.ChainName`, which already knows the chain roster), and construct the Etherscan client
in the cronjob's `run()`. Reintroduce the factory when a second verifier kind exists.

**Benefits.** One fewer package and indirection between the validator and its source; the
"which chains can we validate?" fact lives with the rest of the chain roster.

**Risk / migration.** Trivial; single call site. Counter-argument worth weighing: this is one
of the few places where the AGENTS.md "new chain = registry entry" promise is literally
implemented, and 86 lines is cheap. Rank below F08.1–F08.7.

**Size**: S

---

### F08.11 — `pkg/blockchain` is four unrelated concerns under a name that promises chain access

**Strength**: Worth exploring
**Files**:
- oracle pricing (1,067 of 1,459 src lines): `feed_prices.go` (359), `erc4626_share_prices.go` (304), `curve_lp_prices.go` (176), `oracle.go` (172, minus `ExecutePinned`), `resolve_oracle.go` (54)
- protocol registry: `protocols.go` (260) — Aave/SparkLend deployments, `PoolDataProviderHistory`
- oracle resolvers: `aave_resolver.go`, `sparklend_resolver.go`, `resolver_factory.go` (129)
- one constant: `constants.go:5` `Multicall3` (used by all 13 multicaller construction sites)
- the pin-dispatch helper: `oracle.go:25-30` `ExecutePinned` (5 callers, 2 of them outside this package)

**Problem.** The package a reader opens looking for "how do we talk to the chain" contains
mostly oracle-price fetchers. Its 19 files split cleanly into four groups that share only the
`Multicaller` dependency. Two consequences: `ExecutePinned` — the single most important
convention in this area (F08.1) — is buried in `oracle.go` next to `FetchOraclePrices`, and
`pkg/aavelike` imports the whole oracle-pricing package to reach it
(`aavelike/blockchain_service.go:213`); and the protocol registry is imported by 13 `cmd/`
mains that want nothing else from the package.

**Proposed change.** Split: `pkg/oraclepricing` (the fetchers + validators),
`pkg/protocolregistry` (`protocols.go` + the two resolvers + factory), and let
`ExecutePinned`/`Multicall3` land in the F08.1 pin package. No behaviour change.

**Benefits.** Import graph states intent; the pin helper is discoverable; `cmd/` mains stop
importing oracle pricing to get one address constant.

**Risk / migration.** Pure move; do it as part of F08.1 step 1 so the pin helper lands in its
final home once.

**Size**: M
**Depends on / enables**: F08.1

---

### F08.12 — 13 composition roots repeat the chain-client wiring, and 3 of them silently opt out of archiving

**Strength**: Worth exploring
**Files**:
- 13 `multicall.NewClient` sites: `cmd/workers/{morpho,sparklend,prime-debt,fluid-vault,oracle-price,prime-allocation,psm3}-indexer/main.go`, `cmd/workers/internal/dexbootstrap/bootstrap.go`, `cmd/cronjobs/morpho-v2-bootstrap/main.go`, `cmd/backfillers/{oracle-pricing,morpho-vault,sparklend,aave-like-user-snapshot}*/main.go`
- worked example: `cmd/workers/morpho-indexer/main.go:228-271` vs `cmd/workers/psm3-indexer/main.go:212-224`
- `multicall.NewTelemetry`: 8 of 13 (all 5 backfillers/cronjobs omit it)
- `archivingwire.Bootstrap`: 10 of 13 — **`psm3-indexer`, `dexbootstrap` (dex-indexer) and `morpho-v2-bootstrap` archive nothing**

**Problem.** Each main repeats ~20 lines: `rpchttp.DialEthereum` → `multicall.NewTelemetry` →
`multicall.NewClient(ethClient, blockchain.Multicall3, WithTelemetry(...))` →
`archivingwire.Bootstrap(...)` → `mc = archiveWrap(mc)` → `defer archiveDrain()`, with
divergent error strings ("creating multicall client" / "multicall client" / "creating
multicall client: %w") and two silent opt-outs that are invisible unless you diff the mains.
Nothing in the code says whether `psm3-indexer` not archiving is a decision or an omission.

**Proposed change.** One `chainwire.Open(ctx, chainwire.Config{RPCURL, ChainID, ChainName,
BuildID, Source}) (Multicaller, func(), error)` that dials, instruments, wraps with archiving
and returns the drain — the way `archivingwire.Bootstrap` already consolidates its own slice of
this. Each main calls it once. Make `Source` required so a binary cannot forget archiving
without deleting a field.

**Benefits.** ~200 lines out of `cmd/`; telemetry and archiving become on-by-default rather
than per-main; the F08.1 migration has one wiring point to change instead of 13. Locality:
"how a binary reaches the chain" is one function.

**Risk / migration.** Low. Land `chainwire` alongside the existing calls, convert one main per
PR. Confirm the 3 non-archiving binaries with a maintainer before enabling archiving for them
(it writes to S3 and costs money) — this finding's main value may be surfacing that question.

**Size**: M
**Enables**: F08.1

---

### F08.13 — `VatCaller.ReadDebts` reports per-vault failures in-band and returns `nil` error

**Strength**: Worth exploring
**Files**:
- `internal/ports/outbound/vat_caller.go:22-27` ("Individual vault failures are reported via `DebtResult.Err` rather than failing the entire batch")
- `internal/adapters/outbound/blockchain/vat_caller.go:115-194` — `results[i].Err` at `:153`, `:169`, `:174`, `:181`, `:187`; `results[i].Reverted = true` at `:162`; function returns `nil`
- contrast: `psm3_caller.go:422-430` (`AllowFailure: false`, any failure is a hard error)

**Problem.** Two adapters in the same package, both reading versioned per-block state, take
opposite error stances. `ReadDebts` sets `AllowFailure: true` on all 2N sub-calls and hands the
caller a slice with three distinct outcomes per row (ok / `Reverted` / `Err`), returning no
error. Whether a hole reaches the database now depends entirely on `prime_debt` inspecting
every row — which the root AGENTS.md rule ("Never swallow a failure into partial success… A
sub-result that fails must propagate and stop the whole unit of work") says should not be the
caller's job. The `Reverted` flag has a documented justification (a genuinely absent
ilk/urn is a structural fact); `Err` — an unpack failure or a wrong-typed return — does not.

I did not audit `prime_debt`'s consumption of these rows, so this is scoped as the port
contract only.

**Proposed change.** Split the two outcomes: keep `Reverted` as a structural, documented
absence (and gate it at the call site per the AGENTS.md "gate it structurally" rule), and make
every `Err` path return an error from `ReadDebts` instead of embedding it. Align the port doc
with `PSM3Caller`'s stance.

**Benefits.** One error stance across the two callers in the package; a decode bug stops a
block instead of writing NULLs. Tests: the double-reporting cases collapse.

**Risk / migration.** Needs the `prime_debt` consumer checked first (another agent's area) —
if it already treats `Err` as fatal, this is a pure simplification; if it skips-and-continues,
this is a behaviour change that will start DLQ-ing blocks that currently pass with holes.

**Size**: S
**Depends on**: confirmation from the prime_debt/services area

---

## 4. Cross-area observations

- `services/aavelike_position_tracker/service.go` threads `(chainID, blockNumber, blockHash, blockVersion, blockTimestamp)` through 8 in-file hops, peaking at 11 parameters on `snapshotUserPosition:794`; `morpho_indexer` does the same across `service.go`/`discovery.go`/`metamorpho_handler.go`/`vault_v2_handler.go`/`morpho_blue_handler.go`. Both are prime consumers of F08.1's `entity.Block`.
- The same tuple continues into ~13 domain-entity constructors as `(blockNumber, blockVersion, timestamp)` — `entity.NewBorrowerCollateral` (11 params), `NewProtocolEvent`, `NewMorphoMarketState`, `NewMorphoVaultPosition`, `NewOnchainTokenPrice`, … A `Block`-shaped value would shrink those too.
- `services/fluid_vault_indexer` is number-pinned end to end (F08.1) — a live reorg-correctness gap on `main`, not just a style issue.
- `internal/pkg/uniswapv3/reader.go` defines and parses its own NFT-manager and pool ABIs (`:72`, `:77`) and repeats the count/success/unpack skeleton 4×; it belongs in the F08.2/F08.4 sweeps even though it sits in the dex area.
- `services/curveindexer` and `services/uniswapv3indexer` are the only packages using `shared.RunSnapshotReads`; that abstraction is the closest existing thing to the proposed seam and deserves to be the migration target rather than a curve/uniswap detail.
- `services/oracle_backfill/service.go:477`, `:562`, `:598` fetch block timestamps with three separate `HeaderByNumber` calls; `pkg/blockchain/blocktime` exists as a hash-keyed memo for exactly this and is used only by `morpho-vault-backfill` and `morpho_v2_bootstrap`.
- `alchemy/subscriber.go:406-412` drops a block header when the channel is full (warn + metric), relying on `backfill_gaps` to heal. Worth an explicit decision under the AGENTS.md poison-pill rule; it is currently a silent-by-default data path.
- `internal/testutil` holds 35 files mixing in-process mocks, testcontainer lifecycle, DB templating and a deployable mock node (F08.9) — the whole directory would benefit from the same split.

## 5. Open questions

- Are `psm3-indexer`, `dex-indexer` and `morpho-v2-bootstrap` deliberately excluded from raw SC-call archiving, or is that an omission? (F08.12 hinges on it.)
- Does `prime_debt` treat `DebtResult.Err` as fatal? If yes, F08.13 is free; if no, it is a behaviour change with DLQ consequences.
- Is `fluid_vault_indexer` number-pinning known and accepted (e.g. because `getVaultEntireData` is only read at settled heights), or an oversight? Nothing in the code says.
- Does any caller mutate an `*abi.ABI` returned by `abis.Get…`? I found none, but F08.4's shared-pointer form needs that confirmed (or should return values).
- Was `mockchain` ever intended as an in-process test double, or was `internal/testutil/` only ever a convenient home for it?
- Is the `alchemy` block-payload path deliberately kept off `rpchttp` (e.g. credit accounting, or a per-attempt timeout requirement), or is it just older than the `rpchttp` policy? (VEC-188 predates some of `client.go`.)
