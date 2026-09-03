Status: FINAL
# 09 — Ports & domain layers

Area: `stl-verify/internal/ports/{inbound,outbound}`, `internal/domain/entity` (+ `entity/maple`),
`internal/common`, cross-referenced against `internal/services`, `internal/adapters`, `cmd`.

All interface/implementation counts below come from a `golang.org/x/tools/go/packages` +
`types.Implements` scan of the whole module (`Tests: true`), not from grep. Build verified green
(`go build ./...`, exit 0) at commit `c4e0a8f2`.

## 1. Area map

`ports/outbound` is a single flat Go package of 45 source files declaring **59 interfaces and 44
data structs**. `ports/inbound` is one 32-line file with 2 interfaces. `domain/entity` is a flat
package of 45 source files (87 exported structs) plus one subpackage `entity/maple` (12 files).
`internal/common` holds exactly one package, `sqsutil`.

The intended flow (root `AGENTS.md`: "Dependencies flow inward … domain has no dependencies") is
services → ports → domain. What the code actually does:

```
cmd/*  (34 binaries)
  │  wires 28 separate postgres.NewXxxRepository(pool, logger, buildID, batchSize) structs
  ▼
internal/services/*  ──uses──►  ports/outbound  ──imports──►  jackc/pgx/v5   (18 of 45 files)
                                    │  ▲                      go-ethereum    (15 files)
                    holds 44 structs│  │                      encoding/json  (2 files)
                    incl. BlockState,  │
                    BlockEvent, ReorgEvent, BackfillCursor, MaplePool, TokenInput …
                                       │
                                       └── domain/entity  (87 structs, 81 methods,
                                            52 of which are named exactly like a DB table)
```

Three things are inverted relative to the stated architecture:

1. The **block domain lives in the ports package**, not in `domain/entity`. `BlockState`,
   `BlockEvent`, `ReorgEvent`, `BlockRange`, `BackfillCursor`, `BlockHeader`, `BlockData`,
   `CanonicalBlock` are all `ports/outbound` types. The only test in the whole ports tree guards a
   domain invariant on one of them.
2. **18 of 45 port files import `github.com/jackc/pgx/v5`** and thread `tx pgx.Tx` through 44
   method signatures, so the "outbound port" *names its own driver*.
3. `ports/inbound` and everything downstream of it is dead (§F09.4).

## 2. Metrics

| Metric | Value |
|---|---|
| `ports/outbound` files / LOC | 46 (45 src + 1 test) / 2,263 src LOC |
| `ports/inbound` files / LOC | 1 / 32 |
| Interfaces: outbound / inbound | 59 / 2 |
| Non-interface types declared in `ports/outbound` | **44 structs + 1 string enum** |
| Port files importing `jackc/pgx/v5` | **18 / 45 (40%)** |
| Port methods taking `tx pgx.Tx` | **44** |
| `domain/entity` src files / LOC | 57 (45 flat + 12 maple) / 4,882 |
| `domain/entity` test files / LOC | 42 / 7,202 (1.5× the source) |
| Exported structs in domain | 87 (72 flat + 15 maple) |
| Total methods on domain types | 81 (78 exported) — **36 of 87 structs have zero methods** |
| `New*` constructors in domain | 46, of which **44 return `error`** |
| Domain structs whose snake_case name **is** a DB table | **52 / 87 (60%)** |
| DB surrogate-key fields (`XxxID int64`) inside domain entities | **105**, guarded by 86 `ID <= 0` checks |
| `internal/common` | 1 package (`sqsutil`), 2,249 LOC incl. tests |
| Hand-rolled test doubles implementing an outbound port | **140 distinct types across 37 packages** |
| Real seams (≥2 independent production adapters) | **2** — `Multicaller`, `OracleResolver` |
| Hypothetical seams (exactly 1 production adapter) | **54** |
| Dead ports (0 production implementations or 0 production consumers) | **5** |

Method-count histogram (61 ports): 1 method → 23 ports; 2 → 13; 3 → 9; 4 → 6; 5 → 4; 7 → 1;
11 → 1; 12 → 2; 15 → 1; 24 → 1. **45 of 61 ports (74%) have ≤3 methods.**

Largest port files: `blockstate.go` 240 (24-method interface + 4 structs), `morpho_repository.go`
~200 (15 methods), `maple_graphql_client.go` ~160 (8 structs), `metrics.go` (3 interfaces),
`eventsink.go` 130.

Largest domain files: `curve.go` 431, `uniswap_v3.go` 396, `morpho_adapter.go` 225,
`orderbook.go` 149, `maple/ftl_loan_state.go` 148.

Largest domain functions — **the eleven largest are all `Validate()`**:
`UniswapV3LiquidityEvent.Validate` 84 lines (`uniswap_v3.go:204`),
`AllocationPosition.Validate` 49 (`allocation_position.go:49`),
`PSM3Reserves.Validate` 47 (`psm3_reserves.go:46`),
`FTLLoanState.Validate` 44 (`maple/ftl_loan_state.go:105`),
`UniswapV3Swap.Validate` 43, `UniswapV3PoolState.Validate` 42, `ProtocolEvent.Validate` 38,
`BorrowerCollateral.Validate` 36, `Borrower.Validate` 36, `MorphoMarketPosition.Validate` 33,
`MorphoVaultCap.Validate` 33.

### 2a. Full port census (probe 1)

`prod` = production types implementing it; `test` = distinct hand-rolled test doubles;
`cons` = distinct consuming directories (test dirs counted separately, see notes).
`memory.*` adapters are counted as **test doubles, not production**: `internal/adapters/outbound/memory`
has **zero non-test importers** (verified: `grep -rn 'adapters/outbound/memory' --include='*.go' | grep -v _test.go` → empty).

| Interface | File | Methods | Prod | Test | Cons | Verdict |
|---|---|---|---|---|---|---|
| `Multicaller` | multicaller.go | 3 | 3 (`multicall.Client`, `multicall.DirectCaller`, `archiving.Multicaller`) | 12 | 32 | **real seam** |
| `OracleResolver` | oracle_resolver.go | 1 | 2 (`blockchain.AaveResolver`, `.SparkLendResolver`) | 0 | 1 | **real seam** |
| `BlockCacheReader` | blockcache.go | 5 | 2 (`redis.BlockCache`, `cache.BlockCacheReaderWithFallback` — decorator) | 11 | 14 | hypothetical + decorator |
| `BlockStateRepository` | blockstate.go | **24** | 1 (`postgres`) | 6 | 8 | hypothetical |
| `BlockchainClient` | blockchain_client.go | 12 | 1 (`alchemy.Client`) | 8 | 7 | hypothetical |
| `SQSConsumer` | sqs.go | 5 | 1 (`sqs.Consumer`) | 9 | 19 | hypothetical |
| `TxManager` | tx_manager.go | 1 | 1 (`postgres.TxManager`) | 10 | 15 | hypothetical |
| `TokenRepository` | token_repository.go | 4 | 1 | 4 | 11 | hypothetical |
| `ProtocolRepository` | protocol_repository.go | 2 | 1 | 4 | 10 | hypothetical |
| `S3Reader` | s3_reader.go | 3 | 1 (`s3.Reader`) | 7 | 10 | hypothetical |
| `EventRepository` | event_repository.go | 2 | 1 | 7 | 7 | hypothetical |
| `S3Writer` | s3_writer.go | 2 | 1 (`s3.Writer`) | 2 | 7 | hypothetical |
| `MorphoRepository` | morpho_repository.go | 15 | 1 + 1 decorator in `cmd` | 1 | 5 | hypothetical |
| `BlockCache` | blockcache.go | 7 | 1 (`redis`) | 6 | 9 | hypothetical |
| `OnchainPriceRepository` | onchain_price_repository.go | 12 | 1 | 3 | 6 | hypothetical |
| `EventSink` | eventsink.go | 2 | 1 (`sns.EventSink`) | 4 | 6 | hypothetical |
| `UserRepository` | user_repository.go | 3 | 1 | 1 | 5 | hypothetical |
| `BackupMetricsRecorder` | metrics.go | 2 | 1 (`telemetry.Metrics`) | 2 | 5 | hypothetical |
| `DeadLetterPublisher` | sqs.go | 1 | 1 | 1 | 4 | hypothetical |
| `BlockVerifier` | block_verifier.go | 4 | 1 (`etherscan.Client`; `blockverifier` is a factory) | 1 | 4 | hypothetical |
| `OrderbookProvider` | orderbook_provider.go | 2 | 1 (`orderbook.feedProvider`, 3 configs) | 1 | 4 | hypothetical |
| `ReceiptTokenRepository` | receipt_token_repository.go | 1 | 1 | 1 | 4 | hypothetical |
| `CallArchiver` | call_archiver.go | 1 | 1 (`s3.CallArchiver`) | 5 | 3 | hypothetical |
| `MapleGraphQLRepository` | maple_loan_repository.go | 11 | 1 | 1 | 2 | hypothetical |
| `MapleGraphQLClient` | maple_graphql_client.go | 5 | 1 (`maple.Client`) | 1 | 2 | hypothetical |
| `TransformRunner` | transform_runner.go | 5 | 1 | 1 | 2 | hypothetical |
| `PositionRepository` | position_repository.go | 4 | 1 | 1 | 2 | hypothetical |
| `PriceProvider` | price_provider.go | 4 | 1 (`coingecko.Client`) | 1 | 2 | hypothetical |
| `PriceRepository` | offchain_price_repository.go | 4 | 1 | 1 | 2 | hypothetical |
| `BlockSubscriber` | blockchain.go | 3 | 1 (`alchemy.Subscriber`) | 2 | 3 | hypothetical |
| `FluidVaultRepository` | fluid_vault_repository.go | 3 | 1 | 1 | 3 | hypothetical |
| `UniswapV3Repository` | uniswap_v3_repository.go | 3 | 1 | 1 | 2 | hypothetical |
| `BlockCacheWriter` | blockcache.go | 3 | 1 (`redis`) | 6 | **1 (only `ports/outbound` itself)** | **dead as a seam** |
| `ReorgRecorder` | metrics.go | 3 | 1 (`shared.ServiceTelemetry`) | 1 | 2 | hypothetical |
| `BackfillRecorder` | metrics.go | 3 | 1 (`shared.ServiceTelemetry`) | 1 | 2 | hypothetical |
| `CurveRepository` | curve_repository.go | 2 | 1 | 1 | 2 | hypothetical |
| `AnchorageOperationRepository` | anchorage_repository.go | 2 | 1 (`postgres.AnchorageRepository`) | 1 | 2 | hypothetical |
| `AnchorageSnapshotRepository` | anchorage_repository.go | 1 | 1 (same struct) | 1 | 2 | hypothetical |
| `PSM3Caller` | psm3.go | 2 | 1 (`blockchain.PSM3Caller`) | 1 | 2 | hypothetical |
| `PSM3ReservesRepository` | psm3.go | 1 | 1 | 1 | 2 | hypothetical |
| `PrimeDebtRepository` | prime_debt_repository.go | 2 | 1 | 1 | 2 | hypothetical |
| `PrimeRepository` | prime_repository.go | 2 | 1 | 2 | 3 | hypothetical |
| `VatCaller` | vat_caller.go | 2 | 1 (`blockchain.VatCaller`) | 1 | 2 | hypothetical |
| `Event` | eventsink.go | 4 | 1 (`outbound.BlockEvent` — same package) | 2 | 6 | hypothetical |
| `AllocationRepository` | allocation_repository.go | 1 | 1 | 1 | 2 | hypothetical |
| `DebtTokenRepository` | debt_token_repository.go | 1 | 1 | 1 | 2 | hypothetical |
| `TokenTotalSupplyRepository` | token_total_supply_repository.go | 1 | 1 | 1 | 2 | hypothetical |
| `OrderbookSnapshotRepository` | orderbook_snapshot_repository.go | 1 | 1 | 1 | 3 | hypothetical |
| `BalanceSheetProvider` | balance_sheet_provider.go | 1 | 1 (`skydata.Client`) | 2 | 3 | hypothetical |
| `ReferencePositionProvider` | reference_position_provider.go | 1 | 1 (same `skydata.Client`) | 1 | 2 | hypothetical |
| `PrimeBalanceSheetRepository` | balance_sheet_provider.go | 1 | 1 | 2 | 3 | hypothetical |
| `PrimeReferencePositionRepository` | reference_position_provider.go | 1 | 1 | 1 | 2 | hypothetical |
| `RiskCapitalProvider` | risk_capital_provider.go | 1 | 1 (`sky.Client`) | 1 | 2 | hypothetical |
| `RiskCapitalAllocationProvider` | risk_capital_provider.go | 1 | 1 (same `sky.Client`) | 1 | 2 | hypothetical |
| `PrimeCapitalStackAllocationRepository` | risk_capital_provider.go | 1 | 1 | 1 | 2 | hypothetical |
| `PrimeCapitalStackRepository` | prime_capital_stack_repository.go | 1 | 1 | 1 | 2 | hypothetical |
| `S3Overwriter` | s3_writer.go | 1 | 1 (same `s3.Writer`) | 1 | 3 | hypothetical |
| `S3RangeReader` | s3_reader.go | 1 | 1 (same `s3.Reader`) | 1 | 2 | hypothetical |
| `Repository` | repository.go | 1 | **0** (only `memory.Repository`, test-only pkg) | 2 | 2 | **dead** |
| `VerificationService` | inbound/services.go | 1 | 1 (`services.VerificationService`, never wired) | 0 | 2 | **dead** |
| `HealthChecker` | inbound/services.go | 2 | **0** | 1 | 1 | **dead** |

Notes on the census: `S3Reader`/`S3RangeReader`, `S3Writer`/`S3Overwriter`,
`AnchorageSnapshotRepository`/`AnchorageOperationRepository`,
`BalanceSheetProvider`/`ReferencePositionProvider`,
`RiskCapitalProvider`/`RiskCapitalAllocationProvider`, and
`BlockCache`/`BlockCacheReader`/`BlockCacheWriter` are **pairs/triples split off one single concrete
type each** — 14 interfaces backed by 6 structs. `VerificationService`'s apparent 8 implementations
are structural coincidences (`pgx.Conn`, `http2.ClientConn`, … all have `Ping(ctx) error`), which is
itself a signal: a 1-method port that anything accidentally satisfies is not a type.

## 3. Findings

---

### F09.1 — `ports/outbound` names its own database driver: 44 methods take `tx pgx.Tx`

**Strength**: Strong
**Files**: `internal/ports/outbound/{allocation,debt_token,event,fluid_vault,curve,maple_loan,morpho,position,protocol,receipt_token,token,token_total_supply,uniswap_v3,user,prime_capital_stack}_repository.go`, `reference_position_provider.go`, `risk_capital_provider.go`, `tx_manager.go`
**Problem**: 18 of 45 port files import `github.com/jackc/pgx/v5`, and 44 port methods take
`tx pgx.Tx` as their second parameter. Examples:

- `tx_manager.go:21` — `WithTransaction(ctx context.Context, fn func(tx pgx.Tx) error) error`
- `morpho_repository.go:29` — `GetOrCreateMarket(ctx, tx pgx.Tx, market *entity.MorphoMarket) (int64, error)` (10 of its 15 methods)
- `maple_loan_repository.go:44-90` — all 10 methods
- `curve_repository.go:87` — `SaveBlock(ctx, tx pgx.Tx, w BlockWrites) (stateRows int64, err error)`, doc comment `"persists all of a block's curve rows in one pgx.Batch"`

This is the single mechanical reason the seam count is 2: **no non-Postgres adapter can implement
these ports at all**, and neither can a test double without importing pgx. It also inverts the
dependency rule the root `AGENTS.md` states first ("adapters depend on ports"): here the port
depends on the adapter's driver. `TxManager` is the load-bearing case — it exists to hide the
transaction mechanism and instead publishes it, so every one of its 15 consuming directories and all
10 of its hand-rolled doubles are pgx-shaped.

**Proposed change**: introduce an opaque transaction handle owned by the ports package and make the
postgres adapter the only thing that knows it wraps a `pgx.Tx`:

```go
// ports/outbound
type Tx interface{ private() }              // or a struct with an unexported field
type TxManager interface {
    WithTransaction(ctx context.Context, fn func(Tx) error) error
}
```
The postgres adapter type-asserts `Tx` back to its own `*pgTx` in one helper
(`func unwrap(tx outbound.Tx) (pgx.Tx, error)`) and everything above the adapter stops importing
pgx. `SaveBlock`-style batch methods keep their signature shape; only the parameter type changes.
Land it port-file by port-file behind a temporary alias `type Tx = pgx.Tx` so the whole tree
compiles at every step, then flip the alias to the real interface last.

**Benefits**: locality — the pgx dependency collapses from 18 port files + ~30 service packages to
one adapter package; leverage — a second store (or a nop/recording store for tests) becomes
possible for the first time; tests get better because a fake `TxManager` stops being 10 different
pgx-shaped stubs (§F09.7).
**Risk / migration**: mechanical but wide (18 port files, 28 postgres files, ~30 service packages).
The alias trick makes each PR compile-safe. Risk of a missed unwrap is caught at compile time except
for the one assertion site.
**Size**: L
**Enables**: F09.2, F09.7, F09.8

---

### F09.2 — 22 one-table `XxxRepository` ports are a 1:1 mirror of 22 postgres files; 28 near-identical adapter structs get wired by hand in every binary

**Strength**: Strong
**Files**: every `internal/ports/outbound/*_repository.go` (22 files) against
`internal/adapters/outbound/postgres/*_repository.go` (22 files with **the same basename**);
wiring e.g. `cmd/workers/morpho-indexer/main.go:278-305`
**Problem**: 22 port basenames match a postgres file basename exactly
(`allocation_repository.go`, `anchorage_repository.go`, `curve_repository.go`,
`debt_token_repository.go`, `event_repository.go`, `fluid_vault_repository.go`,
`maple_loan_repository.go`, `morpho_repository.go`, `offchain_price_repository.go`,
`onchain_price_repository.go`, `orderbook_snapshot_repository.go`, `position_repository.go`,
`prime_capital_stack_repository.go`, `prime_debt_repository.go`, `prime_repository.go`,
`protocol_repository.go`, `receipt_token_repository.go`, `token_repository.go`,
`token_total_supply_repository.go`, `tx_manager.go`, `uniswap_v3_repository.go`,
`user_repository.go`). Each is one interface, one adapter, one consuming service.

The 28 postgres structs behind them are the same struct wearing 28 names, and they diverge for no
reason:

| Field | Present on |
|---|---|
| `pool *pgxpool.Pool` | 27 of 28 (`PSM3ReservesRepository` has only `txm`) |
| `logger *slog.Logger` | 26 of 28 (missing: `PrimeRepository`, `UniswapV3Repository`) |
| `buildID buildregistry.BuildID` | 13 of 28 |
| `batchSize int` | 6 of 28 |
| `txm *TxManager` | 5 of 28 |
| `tokenRepo outbound.TokenRepository` | 2 (`AllocationRepository:26`, `TokenTotalSupplyRepository:27`) |

Consequences measured: **86 `postgres.New*` call sites** across `cmd/` and `internal/`; one worker
(`cmd/workers/morpho-indexer/main.go:278-305`) constructs 7 repositories in 28 lines with 7
different argument shapes (`NewEventRepository(logger, buildID)` takes no pool at all;
`NewUserRepository(pool, logger, 0)` and `NewProtocolRepository(pool, logger, buildID, 0)` pass a
bare `0` for `batchSize`). Adding one column to one table currently touches: migration → entity →
port interface → postgres file → service → `cmd` wiring → the service's hand-rolled double. That is
the fan-out the brief's median-7/p90-31 file counts are made of.

Two of them (`AllocationRepository`, `TokenTotalSupplyRepository`) hold `outbound.TokenRepository`
as a field — an adapter depending on a port so it can reach a sibling adapter, which is the
registry-FK resolution (`token` natural key) leaking into the persistence layer.

**Proposed change**: group ports by **capability and bounded context**, not by table. One port per
indexed protocol/domain, each with a small number of block-scoped write methods, e.g.

```go
type MorphoStore interface {                 // replaces MorphoRepository (15) + ReceiptTokenRepository (1)
    SaveBlock(ctx context.Context, tx Tx, w MorphoBlockWrites) (rows int64, err error)
}
type RegistryStore interface {               // replaces Token/User/Protocol/DebtToken/ReceiptToken (11 methods, 5 ports)
    ResolveTokens(ctx, tx, []TokenInput) (map[common.Address]int64, error)
    ResolveUsers(ctx, tx, []entity.User) (map[common.Address]int64, error)
    ResolveProtocol(ctx, tx, ProtocolInput) (int64, error)
}
```
`CurveRepository` and `UniswapV3Repository` already have exactly this shape
(`SaveBlock(ctx, tx, BlockWrites) (int64, error)` — `curve_repository.go:87`,
`uniswap_v3_repository.go:42`) and are the two cheapest ports in the tree to consume; they are the
existing proof this works. On the adapter side, replace the 28 structs with one `postgres.Store`
holding `pool/logger/buildID/batchSize` once, and give it method sets per file
(`store_morpho.go`, `store_registry.go` …). Wiring per binary drops from 7 constructors to one.

**Benefits**: locality — a schema change touches migration + one store file + one `BlockWrites`
struct; leverage — `cmd` wiring collapses from 86 constructor calls to ~34; the `batchSize`/
`buildID`/`logger` divergence disappears by construction; tests replace N repo doubles with one
recording store.
**Risk / migration**: L–XL, but naturally incremental — the ports can be merged one bounded context
at a time (morpho, curve+uniswap already done, prime, maple, aavelike, registry) and each merge is
its own PR. The `RegistryStore` merge is the highest-value and lowest-risk starting point because
`Token`/`User`/`Protocol` repositories are consumed by 5-7 services each, so it removes the most
duplicate wiring. Main hazard: `postgres.Store` becoming a god type — mitigate by keeping one file
per context and never letting the struct grow context-specific fields.
**Size**: XL (as an epic; each context merge is M)
**Depends on**: F09.1 (do the `pgx.Tx` swap first, or you re-touch the same signatures twice)

---

### F09.3 — Block identity has no type: 125 functions thread ≥3 of (chainID, number, hash, version, timestamp) as separate parameters

**Strength**: Strong
**Files**: `internal/ports/outbound/blockcache.go:22-47` (6 methods),
`internal/adapters/outbound/redis/blockcache.go:145,286,310,315,373-458` (12),
`internal/adapters/outbound/cache/reader_with_fallback.go:69-99` (5),
`internal/domain/entity/{morpho_adapter_state,morpho_market_position,morpho_market_state,morpho_vault_cap,morpho_vault_fee,morpho_vault_position,morpho_vault_state,onchain_price,protocol_event}.go` (9 constructors),
plus `internal/services/morpho_indexer` (31), `aavelike_position_tracker` (14),
`curveindexer` (9), `uniswapv3indexer` (7), `internal/testutil` (8)
**Problem**: `grep -rn 'type Block\(Ref\|ID\|Identity\|Key\|Pointer\|Coord\)'` over the repo returns
**nothing**. There is no type for "which block are we talking about", so the tuple is spelled out at
every boundary. An AST scan for functions/methods taking ≥3 of `{chainID}`, `{blockNumber|number}`,
`{blockHash|hash}`, `{version}`, `{timestamp|blockTimestamp}` as *separate named parameters* finds
**125 non-test functions** in 20 directories.

Two distinct spellings of the same idea coexist:
- cache layer: `(chainID, blockNumber, version)` — `blockcache.go:22` `GetBlock(ctx, chainID int64, blockNumber int64, version int)`
- entity constructors: `(blockNumber, blockVersion, timestamp)` — `morpho_market_state.go:29` `NewMorphoMarketState(..., blockNumber int64, blockVersion int, timestamp time.Time, ...)`

and `chainID` itself is typed `int64` in 160 places and `int` in 19.

The invariant that makes this dangerous is already documented in one place —
`ports/outbound/eventsink.go:88-105`, `BlockEvent.ParsedBlockHash()`: an empty hash must never
become the zero hash, because "state-read callers treat the zero hash as the number-pinned backfill
fallback". That is a rule about a *value*, enforced by a method on a struct in the ports package,
which 125 call sites bypass entirely by passing loose ints.

**Proposed change**: add a value type to `domain/entity` (the one place that has no dependencies)
and thread it instead:

```go
package entity
type BlockRef struct {                    // constructed once, validated once
    ChainID   ChainID                     // named type over int64
    Number    int64
    Version   int
    Hash      common.Hash                 // zero hash == "pin by number" (backfill/CLI)
    Timestamp time.Time
}
func NewBlockRef(...) (BlockRef, error)   // absorbs ParsedBlockHash's guard
func (r BlockRef) CacheKey(dataType string) string   // absorbs redis/blockcache.go:286 key()
func (r BlockRef) IsHashPinned() bool
```
`BlockCacheReader` becomes `GetBlock(ctx, ref entity.BlockRef) ([]byte, error)` — 4 params → 1, and
the cache-key convention documented in `stl-verify/AGENTS.md` becomes a method instead of a
convention re-implemented in `redis/blockcache.go:286` and `pkg/rawsckey`.

**Benefits**: leverage — the reorg-correctness guard becomes unbypassable rather than one method on
one struct; locality — the cache-key format lives in exactly one place; the `int` vs `int64` chainID
split gets settled by a named type; every one of the 125 signatures gets shorter and loses its
argument-order hazard (`(number, version)` vs `(version, number)` is currently a silent bug).
**Risk / migration**: land `BlockRef` + constructors first with no callers, then convert
directory by directory starting at `ports/outbound/blockcache.go` (6 methods, 3 adapters,
17 doubles) — that one conversion covers 23 of the 125 sites. Entity constructors can keep their
current signatures during migration by delegating. Breakage is compile-time, not runtime.
**Size**: L
**Enables**: F09.5; **depends on** F09.6 (BlockEvent's home)

---

### F09.4 — The entire inbound side of the hexagon is dead code

**Strength**: Strong
**Files**: `internal/ports/inbound/services.go` (whole file), `internal/ports/outbound/repository.go`
(whole file), `internal/services/verification_service.go`,
`internal/adapters/outbound/memory/repository.go`, `internal/adapters/inbound/http/`
(`handler.go`, `health.go`)
**Problem**: none of it is reachable from any of the 34 binaries.

- `inbound.VerificationService` still carries its template comment: `// Add your use case methods here / // Example: / // Verify(ctx …)` and one real method, `Ping`.
- `outbound.Repository` (`repository.go:9-15`) likewise: `// Add your repository methods here`, one `HealthCheck` method. Its only implementation is `memory.Repository`, in a package with **zero non-test importers**.
- `inbound.HealthChecker` has **zero implementations** anywhere — `grep -rn 'IsReady' --include='*.go' .` returns only the interface declaration, two call sites inside `http/health.go`, and `health_test.go:17`'s `mockHealthChecker`. Its doc comment claims `// Implementations: / //   - LiveService: Ready after first block processed` — that type does not have the method.
- `NewHealthServer` (`http/health.go:65`) is called only from `http/health_test.go` (3 call sites). The `/health`, `/health/ready`, `/health/live` routes at `health.go:83-85` are the only health endpoints in the Go service and nothing serves them.
- `NewHandler` (`http/handler.go:22`) has no caller outside its own package.

So `ports/inbound` — the entire "primary port" half of the stated architecture — is 32 lines of
scaffolding that has never been used, and the `AGENTS.md` recipe "New Use Case: 1. Add method to
inbound port interface in `internal/ports/inbound/`" describes a workflow nobody has ever run.

**Proposed change**: delete `internal/ports/inbound`, `internal/ports/outbound/repository.go`,
`internal/services/verification_service.go`, `internal/adapters/outbound/memory/repository.go`, and
`internal/adapters/inbound/http/handler.go`. For `health.go`, first answer the open question below;
if k8s probes really do target these paths through some other binary, wire it and give
`HealthChecker` its implementation — otherwise delete it too and drop the `adapters/inbound` layer
from `AGENTS.md`. Rewrite the "New Use Case" recipe to describe what the repo actually does
(add a worker under `cmd/workers/`, per `CONTRIBUTING.md`).
**Benefits**: removes a whole architectural layer from every newcomer's and every agent's mental
model; kills 3 of the 5 dead ports; stops `VerificationService`'s 1-method shape from being
accidentally satisfied by `pgx.Conn` and `http2.ClientConn` in any future `types.Implements` audit.
**Risk / migration**: near-zero for everything except `health.go` — verify the k8s probe targets
first (`k8s/` overlays are outside my area).
**Size**: S

---

### F09.5 — `ports/outbound` holds 44 data structs, including the block domain; `domain/entity` holds the persistence model

**Strength**: Strong
**Files**: `internal/ports/outbound/blockstate.go:9-80` (`BlockState`, `ReorgEvent`, `BlockRange`,
`BackfillCursor`), `eventsink.go:42-105` (`BlockEvent` + its methods + the only ports test),
`blockchain.go:6`, `blockchain_client.go:9`, `block_verifier.go:15`, `call_archiver.go:11,27`,
`maple_graphql_client.go:14-137` (8 structs), `curve_repository.go:15-80` (4),
`uniswap_v3_repository.go:13,28`, `price_provider.go:19-58` (6), `risk_capital_provider.go:17,55`,
`reference_position_provider.go:15`, `balance_sheet_provider.go:12`, `token_repository.go:15`,
`onchain_price_repository.go:12`, `transform_runner.go:50,59`, `sqs.go:12`, `s3_reader.go:10`,
`multicaller.go:26,32`
**Problem**: the ports package declares 103 types, of which only 59 are interfaces. The other 44 are
data. Two things follow.

First, **the most important domain concepts in a block-watcher live in the ports layer**:
`BlockState`, `BlockEvent`, `ReorgEvent`, `BlockRange`, `BackfillCursor`. `BlockEvent` carries
`json` tags (`eventsink.go:43-72`) — SNS transport shape — *and* the reorg-correctness invariant
(`ParsedBlockHash`, `eventsink.go:88-105`) *and* the FIFO dedup rule (`DeduplicationID`,
`eventsink.go:83-85`). It is simultaneously a wire format, a domain entity and a port type.

Second, `domain/entity` is where the *database* lives: **52 of 87 exported structs are named exactly
like a DB table** (`allocation_position`, `borrower_collateral`, `curve_stableswap_state`,
`maple_loan_state`, `morpho_vault_cap`, `protocol_event`, `uniswap_v3_tick`, …), they carry **105
surrogate-key fields** (`ProtocolID int64` ×14, `CurvePoolID` ×12, `PoolID` ×10, `TokenID` ×7 …),
and their `Validate()` methods enforce **86 `ID <= 0`** checks — a database invariant, not a domain
one. `maple.Pool` (`entity/maple/pool.go:11-18`) is a row: `ChainID, ProtocolID, Address []byte,
Name, AssetTokenID int64, IsSyrup`, with `Validate()` asserting `ProtocolID > 0` and
`AssetTokenID > 0`. A `Pool` cannot be constructed until the database has assigned ids to two other
rows.

The two layers have swapped jobs, and the domain says so itself: `entity/maple/doc.go` describes
its own package as "registry rows and their per-cycle state snapshots" — table rows and snapshot
rows. The same doc claims "Like the parent entity package, it has no dependencies outside the
standard library"; `go list -f '{{.Imports}}' ./internal/domain/...` shows the parent importing
`github.com/ethereum/go-ethereum/common` and `.../crypto` across 15 files (plus `encoding/json` in
`protocol_event.go`, `curve.go`, `uniswap_v3.go`, and `context` in `debt_types.go`). `maple` is the
only genuinely stdlib-only package in the domain.

**Proposed change**: move block identity and block lifecycle types into `domain/entity`
(`entity.BlockRef` per F09.3, plus `entity.BlockState`, `entity.ReorgEvent`, `entity.BlockRange`,
`entity.BackfillCursor`), keep the JSON-tagged wire struct in the SNS adapter and let the port carry
the domain type. Leave genuinely transport-shaped DTOs (`MaplePool`, `PriceData`,
`RiskCapitalAllocationRow`, `SwapInput`) in the ports package but name them so — they *are*
the adapter's contract and their doc comments already say so ("as decimal strings exactly as
reported", `risk_capital_provider.go:14-16`). Split `domain/entity` into per-context packages
(`entity/morpho`, `entity/curve`, `entity/prime`, alongside the existing `entity/maple`) so the flat
45-file package stops being the shared dumping ground.
**Benefits**: the dependency arrow finally points inward; `ParsedBlockHash`'s guard sits in the
layer nothing can bypass; the flat `entity` package's 45 files stop forcing every service to import
every other context's structs.
**Risk / migration**: mostly import churn, all compile-time. Do it after F09.3 so `BlockRef` is
already the thing being moved. `BlockEvent`'s JSON tags are the one behavioural risk — the SNS
message format must not change, so keep the tagged struct byte-identical in the adapter and add a
conversion.
**Size**: L
**Depends on**: F09.3

---

### F09.6 — 221 hand-rolled validation checks and 46 near-identical `Validate()` methods; the only shared helpers are 3 unexported functions inside `entity/maple`

**Strength**: Strong
**Files**: `internal/domain/entity/maple/validation.go:18-46` (the 3 helpers),
`internal/domain/entity/uniswap_v3.go:204-288` (84-line `Validate`),
`internal/domain/entity/allocation_position.go:49-98`, `psm3_reserves.go:46-93`,
`maple/pool.go:37-52`, `maple/loan.go:55-77`, and 40 more
**Problem**: the domain's entire behaviour is validation, and it is copy-pasted. Phrase histogram
across non-test `domain/entity`:

| Message | Occurrences |
|---|---|
| `%s must be positive, got %d` | 112 |
| `%s must be non-negative, got %d` | 27 |
| `%s must not be nil` | 23 (+ 12 variants) |
| `%s must not be zero` | 22 |
| `%s must not be empty` | 22 |
| `%s is required` | 19 |
| `%s must be 20 bytes, got %d` | 9 (+ 2 `address length: expected 20, got %d`) |

`maple/validation.go` shows the codebase already knows the fix: `requireNonNegBigInt`,
`requireNonNegBigIntIfSet`, `requireNonNegInt64` — three helpers, 29 lines, used only inside the
`maple` package. The other 45 flat-package entities re-inline the same checks. Put side by side,
`maple/pool.go:37-52` and `maple/loan.go:55-77` differ only in field names:

```go
if p.ChainID <= 0    { return fmt.Errorf("chainID must be positive, got %d", p.ChainID) }
if p.ProtocolID <= 0 { return fmt.Errorf("protocolID must be positive, got %d", p.ProtocolID) }
if len(p.Address) != 20 { return fmt.Errorf("address must be 20 bytes, got %d", len(p.Address)) }
```
`Validate()` being the eleven largest functions in the domain (§2) is the same fact from the other
direction.

**Proposed change**: promote the `maple` helpers to `entity` (exported or not, same package tree),
extend them to cover the six recurring shapes, and let a `Validate()` read as a list:

```go
func (p *Pool) Validate() error {
    return errors.Join(
        requirePositive("chainID", p.ChainID),
        requirePositive("protocolID", p.ProtocolID),
        requireAddress20("address", p.Address),
        requirePositive("assetTokenID", p.AssetTokenID),
    )
}
```
`errors.Join` also fixes a real behaviour gap: today the first failure short-circuits, so a caller
fixing one bad field learns about the next one only on the next run.
**Benefits**: ~600 lines of validation collapse to ~150 plus one helper file; the message wording
becomes consistent (currently `must be 20 bytes, got %d` vs `address length: expected 20, got %d`);
the 42 domain test files (7,202 lines, 1.5× the source) shrink because per-entity tests stop
re-testing "chainID must be positive" 41 times and one helper test covers it.
**Risk / migration**: entity-by-entity, purely additive until the last one. Error *strings* change
for the joined case — check no test or alert matches on exact validation text before landing.
**Size**: M

---

### F09.7 — 140 hand-rolled test doubles for 59 ports, alongside 13 canonical ones in `testutil` that nobody is required to use

**Strength**: Strong
**Files**: `internal/testutil/` (13 doubles incl. `MockBlockCache`, `MockMulticaller`,
`MockTxManager`, `MockTokenRepository`, `MockProtocolRepository`, `MockEventRepository`,
`MockSQSConsumer`, `MockBlockchainClient`); duplicates in
`internal/services/curveindexer` (11), `reference_capital_indexer` (10), `live_data` (10),
`backfill_gaps` (9), `dexconsumer` (8), `raw_data_backup` (7), `fluid_vault_indexer` (7),
`cmd/workers/internal/dexbootstrap` (7), `uniswapv3indexer` (5), `pkg/blockchain/archiving` (5)
**Problem**: a `types.Implements` scan finds **140 distinct non-production types** satisfying an
outbound port, spread over 37 packages. The worst offenders:

- `Multicaller` (3 methods): 12 doubles — `stubMulticaller`, `mockHashMulticaller`, `stubInner`, `capturingMulticaller`, `fakeMulticaller`, `hashRecordingMulticaller`, `txCheckingMulticaller`, `curveMockMulticaller`, `recordingMulticaller`, `truncatingTickMulticaller`, + `testutil.MockMulticaller`
- `TxManager` (**1 method**): 10 doubles — `stubTxManager` ×3, `fakeTxManager` ×2, `countingTxManager` ×2, `inTxTrackingTxManager`, `runningTxManager`, + `testutil.MockTxManager`
- `BlockCacheReader` (5): 11 doubles across 9 packages
- `SQSConsumer` (5): 9 doubles
- `BlockchainClient` (12): 8 doubles
- `S3Reader` (3): 7; `EventRepository` (2): 7; `BlockStateRepository` (24): 6; `CallArchiver` (1): 5

Ten doubles for a **one-method** interface is the tell: the double is not varying behaviour, it is
varying *bookkeeping* (count calls, capture args, record ordering, fail on the Nth call). Every
package re-implements the same three concerns. The naming is also unstandardised —
`stub*`/`mock*`/`fake*`/`capturing*`/`recording*`/`counting*` are used interchangeably for the same
role.

**Proposed change**: make `internal/testutil` the single home for port doubles and give each one the
three knobs every hand-rolled variant reinvents: a recorded call log, a per-method error injector,
and a per-method function override. One generic helper covers most of it:

```go
// testutil
type Stub[Req, Resp any] struct { Calls []Req; Err error; Fn func(Req) (Resp, error) }
```
Then `testutil.MockTxManager` with `.FailOnCall(2)`, `.Calls` and `.InTx` replaces all 10
`TxManager` doubles. Enforce with a CI check in the spirit of the existing
`make shared-container-check`: fail the build on a new non-test-util type satisfying an outbound
port. F09.1 and F09.2 both shrink this problem independently (fewer ports to double, and a
pgx-free `Tx` makes a shared `TxManager` double trivial).
**Benefits**: locality — one place to update when a port changes, instead of 10; today a port method
addition breaks N doubles in N packages, which is exactly the p90-31-files fan-out; new tests get a
double with call-recording and error injection for free rather than 30 lines of boilerplate.
**Risk / migration**: zero production risk (test-only). Do it per port, highest double-count first
(`TxManager` → `Multicaller` → `BlockCacheReader` → `SQSConsumer` covers 42 of the 140).
**Size**: L (but each port is S)
**Depends on**: benefits from F09.1

---

### F09.8 — "Interface segregation" has produced 45 ports with ≤3 methods, 14 interfaces over 6 structs, and one 24-method god port

**Strength**: Strong
**Files**: `internal/ports/outbound/s3_reader.go:17,33`, `s3_writer.go:9,22`,
`anchorage_repository.go:11,17`, `balance_sheet_provider.go:28,35`,
`reference_position_provider.go:40,52`, `risk_capital_provider.go:40,77,89`,
`blockcache.go:19,41,54`, `metrics.go:11,75,101`, `blockstate.go:81`
**Problem**: `stl-verify/AGENTS.md:24` says "Define ports as small, focused interfaces. Prefer
multiple small interfaces over one large one." Applied literally, that has produced:

- **23 ports with exactly one method**, 13 with two, 9 with three — 45 of 61 at ≤3.
- **14 interfaces backed by 6 concrete types.** `s3.Reader` implements both `S3Reader` and `S3RangeReader`; `s3.Writer` both `S3Writer` and `S3Overwriter`; `postgres.AnchorageRepository` both `AnchorageSnapshotRepository` and `AnchorageOperationRepository`; `skydata.Client` both `BalanceSheetProvider` and `ReferencePositionProvider`; `sky.Client` both `RiskCapitalProvider` and `RiskCapitalAllocationProvider`; `shared.ServiceTelemetry` both `ReorgRecorder` and `BackfillRecorder`; `redis.BlockCache` all three of `BlockCache`/`BlockCacheReader`/`BlockCacheWriter`. In every case the split is not swapping anything — one struct is constructed and handed to one caller.
- `BlockCacheWriter` (`blockcache.go:41`) has **exactly one referencing directory: `ports/outbound` itself** — the composite `BlockCache` embeds it. No service, adapter, or test ever names it. It is a seam with no sides.
- Meanwhile the rule was not applied where it would have helped: `BlockStateRepository` has **24 methods** in one interface (`blockstate.go:81-237`), mixing block persistence, reorg handling, backfill watermarks, gap-finding, chain-integrity verification and publish tracking — six responsibilities, consumed by three services that each use a different subset.

Applying the deletion test to a representative one-method port: delete
`PrimeCapitalStackAllocationRepository` (`risk_capital_provider.go:89`, 1 method,
1 adapter, 1 consumer) and complexity does not reappear anywhere — `reference_capital_indexer`
would call `postgres.Store.SaveCapitalStackAllocations` directly. It is a pass-through. That is true
of 22 of the 23 one-method ports (`Multicaller`'s sibling `OracleResolver` being the exception —
two real implementations).

Cost/benefit of the file-per-interface convention specifically: 45 files for 59 interfaces means the
average port file is **50 lines**, and locating a capability requires knowing its file name. There is
no offsetting benefit — Go does not need one interface per file, and the flat package means all 103
types are in one namespace anyway (hence `TokenInput` in `token_repository.go` and `TokenInfo` in
`onchain_price_repository.go` coexisting, §F09.9).

**Proposed change**: reverse the rule's direction — **one port per capability the caller actually
needs, one file per bounded context.** Merge the 14-over-6 splits back into 6 interfaces
(`S3Store`, `AnchorageStore`, `SkyDataProvider`, `RiskCapitalProvider`, `ServiceRecorder`,
`BlockCache`), and *split* `BlockStateRepository` along its six real responsibilities so
`live_data`, `backfill_gaps` and `data_validator` each depend on the ~6 methods they use. Amend
`stl-verify/AGENTS.md:24` to say what the codebase learned: segregate by *caller need*, not by
method count; a one-method port with one adapter and one caller is a function call.

**Benefits**: locality — one file per context instead of 45 files averaging 50 lines; the
`BlockStateRepository` split is the one that pays for itself immediately, because its 6 test doubles
each stub 24 methods to exercise 3.
**Risk / migration**: mechanical and compile-checked. The `BlockStateRepository` split is the only
part with design judgement in it; do the 14→6 merges first as a warm-up.
**Size**: M for the merges, M for the `BlockStateRepository` split
**Depends on / enables**: overlaps F09.2 (same files); do F09.2's context grouping and this falls out

---

### F09.9 — One value, five representations: addresses, token metadata, chain ids

**Strength**: Strong
**Files**: addresses — across `internal/domain/entity/*.go`; token metadata —
`ports/outbound/token_repository.go:15`, `ports/outbound/onchain_price_repository.go:12`,
`services/fluid_vault_indexer/blockchain_service.go:30`,
`services/morpho_indexer/blockchain_service.go:73`, `pkg/aavelike/blockchain_service.go:29`;
maple DTO — `ports/outbound/maple_graphql_client.go:14` vs `domain/entity/maple/loan.go:20`
**Problem**:

*Addresses* — within `domain/entity` alone, an Ethereum address is spelled 5 ways:
`common.Address` (16 fields), `[]byte` (20 fields, e.g. `maple.Pool.Address`,
`MorphoVaultFee.PerformanceFeeRecipient`, `DebtToken.VariableDebtAddress`), `string` (4),
`*common.Address` (2), `*string` (1). The `[]byte` variants are the reason 9 `Validate()` methods
hand-roll `len(x) != 20` — a check `common.Address` makes structurally impossible.

*Chain identity* — `chainID int64` in 160 places, `chainID int` in 19. No named type, no enum,
despite `entity/chain.go` existing and `chain` being a registry table.

*Token metadata* — 5 structs for the same 2-3 fields:

| Type | Fields |
|---|---|
| `outbound.TokenInput` (`token_repository.go:15`) | `ChainID int64, Address common.Address, Symbol string, Decimals int, CreatedAtBlock *int64` |
| `outbound.TokenInfo` (`onchain_price_repository.go:12`) | `Address []byte, Decimals int` |
| `fluid_vault_indexer.TokenMetadata` | `Symbol string, Decimals int` |
| `morpho_indexer.TokenMetadata` | `Symbol string, Decimals int` — **byte-identical to the above** |
| `aavelike.TokenMetadata` | `Symbol string, Decimals int, Name string` |

*Maple DTOs* — `outbound.MapleLoanMeta` (`maple_graphql_client.go:14-20`) is a field-for-field
duplicate of `maple.LoanMeta` (`entity/maple/loan.go:20-27`); only the position of `Location`
differs. Six identical string fields, two doc comments, one conversion function somewhere in
between.

*Amounts* are the one place the convention holds: 108 `*big.Int` fields against 18 `string`
(deliberate — the prime/risk-capital feeds keep upstream decimal text raw, documented at
`risk_capital_provider.go:14-16`), 8 `[]*big.Int`, 1 `*float64`
(`offchain_price.go:109 MarketCapUSD`, the sole `float64` in the domain and a violation of
`AGENTS.md`'s "Wei / token amounts are `big.Int`, never `float64`" — though a market cap arguably
is not a token amount).

`entity/decimal.go` is worth calling out as the counter-example: 130 lines of genuinely deep,
exactly-correct decimal-string comparison (`IsCanonicalDecimal`, `IsZeroDecimal`, `CompareDecimal`)
with a doc comment explaining precisely why float64 is wrong. It has 5 call sites, all orderbook.
The codebase knows how to write a value type; it has done it once.

**Proposed change**: pick `common.Address` as the single in-memory address representation (convert
at the postgres boundary, which is the only place that wants `[]byte`); add
`type ChainID int64` in `entity` and use it everywhere; collapse the 5 token-metadata structs to one
`entity.TokenMetadata{Symbol, Name string, Decimals int}` plus `outbound.TokenInput` for the
registry-resolve call; delete `outbound.MapleLoanMeta` in favour of `maple.LoanMeta`.
**Benefits**: 9 `len(addr) != 20` checks disappear structurally; the 3 `TokenMetadata` copies stop
drifting (they already have: one grew a `Name`); one fewer conversion layer between the maple
adapter and the maple entities.
**Risk / migration**: `common.Address` ⟷ `[]byte` conversion at the postgres boundary must be
audited once per column — a wrong conversion is a silently wrong address, so pair the change with a
round-trip test per repository. Do `ChainID` and the `TokenMetadata` merge first; they are pure
renames.
**Size**: M (`ChainID` + `TokenMetadata` + `MapleLoanMeta`: S each; the address unification: M)

---

### F09.10 — The one test in `ports/outbound` is a domain-invariant test filed in the wrong layer

**Strength**: Worth exploring
**Files**: `internal/ports/outbound/eventsink_test.go` (78 lines, 5 cases),
`internal/ports/outbound/eventsink.go:88-105`
**Problem** (probe 7): the single test in the whole ports tree is
`TestBlockEvent_ParsedBlockHash`. It exists because `BlockEvent.ParsedBlockHash()` carries the most
consequential invariant in the system, quoted from the source:

> "An empty hash must never resolve to the zero hash: state-read callers treat the zero hash as the
> number-pinned backfill fallback, so a live event with no hash would silently downgrade off
> hash-pinning (VEC-471)." … "This is the single guard that keeps VEC-471's reorg-correctness honest
> across every indexer."

So a *port* package owns a reorg-correctness invariant, a hex-parsing routine, and the only test in
its layer. The test is good — the five cases (valid, empty, short, no-`0x`, non-hex) are exactly
right, and the two comments explaining *why* zero-padding is dangerous are model comments. It is in
the wrong place, and its existence is the clearest single piece of evidence for F09.5: if
`ports/outbound` needed no tests, it would contain no behaviour; it contains behaviour, so it is not
a ports package.

Note also that the guard is opt-in — a caller that reads `event.BlockHash` directly bypasses it, and
F09.3's 125 loose-tuple signatures are exactly the population that can.
**Proposed change**: move `BlockEvent`'s identity fields and this method into
`entity.BlockRef`/`NewBlockRef` (F09.3), taking the test and its comments with them. The port then
carries the domain type and the invariant is enforced at construction, not at an optional accessor.
**Benefits**: the guard becomes unbypassable; `ports/outbound` becomes a package of interfaces with
nothing to test, which is the correct end state.
**Risk / migration**: none beyond F09.3.
**Size**: S (as part of F09.3)
**Depends on**: F09.3, F09.5

---

### F09.11 — `internal/common/` is a second, one-package "shared code" root next to `internal/pkg/`

**Strength**: Worth exploring
**Files**: `internal/common/sqsutil/` (5 src files, 1,164 src LOC + 1,085 test LOC), consumed by
`cmd/workers/dex-indexer/main.go` and 8 services
**Problem**: `internal/common` contains exactly one package. The repo also has `internal/pkg/` with
many (`lifecycle`, `telemetry`, `blockchain`, `aavelike`, `uniswapv3`, `rawsckey`,
`buildregistry`, …). Nothing distinguishes the two roots, and `common` is a name Go style guides
single out as meaningless. `sqsutil` itself is *good* — `process_loop.go` (10.6KB) is genuinely deep
(one small interface over the whole SQS FIFO receive/settle/release/drain lifecycle) and is used by 9
services, which is exactly the shape the rest of this report is asking for. Its problem is only its
address. `internal/pkg/lifecycle/shutdown_budget_test.go` importing it is the tell — the
shutdown-budget concern spans both roots.

Also worth noting for probe 8: `sqsutil` is the counterexample to `AGENTS.md:24`. It succeeds
*because* one interface hides a lot of behaviour, not because it was split into small ones.
**Proposed change**: `git mv internal/common/sqsutil internal/pkg/sqsutil` and delete
`internal/common/`. Pure import rewrite, ~14 files.
**Benefits**: one shared-code root; removes a directory whose name carries no information.
**Risk / migration**: none — mechanical rename, compile-checked.
**Size**: S

---

### F09.12 — Consolidated shape: 59 ports and 45 files → ~16 ports in ~10 files

**Strength**: Worth exploring (this is the synthesis of F09.1/2/3/5/8, offered as a target)
**Problem** (probe 9): today's shape is 59 interfaces / 44 structs / 45 files, 2 of which are real
seams. Grouped by capability, the same behaviour needs roughly:

| File | Interfaces | Replaces | Methods |
|---|---|---|---|
| `block.go` | `BlockCache`, `BlockReader`, `BlockSubscriber`, `BlockVerifier` | BlockCache×3, BlockchainClient, BlockSubscriber, BlockVerifier | ~24 |
| `blockstate.go` | `BlockLedger`, `BackfillLedger`, `ReorgLedger` | BlockStateRepository (24 in one) | 24 |
| `store_registry.go` | `RegistryStore` | Token, User, Protocol, DebtToken, ReceiptToken repos | ~11 |
| `store_lending.go` | `MorphoStore`, `AaveLikeStore`, `FluidStore`, `MapleStore` | Morpho, Position, Event, FluidVault, MapleGraphQL repos | ~12 (block-scoped `SaveBlock`) |
| `store_dex.go` | `DexStore` | Curve + UniswapV3 repos (already `SaveBlock`-shaped) | 2 |
| `store_prime.go` | `PrimeStore` | Prime, PrimeDebt, PrimeBalanceSheet, PrimeCapitalStack, PrimeCapitalStackAllocation, PrimeReferencePosition repos (6 ports, 8 methods) | ~6 |
| `store_price.go` | `PriceStore` | OnchainPrice + PriceRepository + TokenTotalSupply + OrderbookSnapshot repos | ~17 |
| `feeds.go` | `PriceFeed`, `SkyFeed`, `OrderbookFeed`, `MapleFeed` | PriceProvider, RiskCapital×2, BalanceSheet, ReferencePosition, OrderbookProvider, MapleGraphQLClient | ~15 |
| `chainio.go` | `Multicaller`, `OracleResolver`, `ContractCaller` | Multicaller, OracleResolver, PSM3Caller, VatCaller | ~8 |
| `infra.go` | `TxManager`, `ObjectStore`, `Queue`, `EventSink`, `CallArchiver`, `Recorder` | TxManager, S3×4, SQS×2, EventSink, CallArchiver, metrics×3 | ~20 |

≈16 interfaces in 10 files, down from 59 in 45. Both real seams survive intact
(`Multicaller`, `OracleResolver`); the two decorator seams (`archiving.Multicaller`,
`cache.BlockCacheReaderWithFallback`) survive because their interfaces are unchanged. Every merged
port keeps the same method bodies on the adapter side — the change is which interface names them.
**Benefits / Risk / Size**: XL epic; see the component findings. Sequence:
F09.4 (delete dead, S) → F09.11 (move `common`, S) → F09.1 (`pgx.Tx` → `Tx`, L) →
F09.3 (`BlockRef`, L) → F09.6 (validation helpers, M) → F09.8 merges (M) →
F09.2 store grouping (XL, one context per PR) → F09.5 (entity/ports type migration, L) →
F09.7 (test doubles, tracks F09.2).

## 4. Cross-area observations

- `internal/adapters/outbound/memory` (4 files, `BlockCache`/`BlockStateRepository`/`EventSink`/`Repository`) has **zero non-test importers** — it is a test-double package sitting in the adapters tree, which inflates every "we have two adapters" claim.
- `internal/adapters/outbound/postgres` is 36 files / 8,889 LOC, of which `blockstate_repository.go` is 1,218, `curve_repository.go` 864, `uniswap_v3_repository.go` 703, `morpho_repository.go` 691, `maple_loan_repository.go` 652 — five files are 47% of the adapter.
- `postgres.AllocationRepository:26` and `postgres.TokenTotalSupplyRepository:27` hold `outbound.TokenRepository` as a field: an adapter reaching a sibling adapter through a port, to do registry-FK resolution.
- `internal/adapters/outbound/orderbook` has 3 constructors (`NewKrakenProvider`, `NewOKXProvider`, `NewCoinbaseProvider`) that all return the same `feedProvider` with different configs — the per-venue variation is data, not a seam, and it is the cleanest example of that pattern in the repo.
- `blockverifier/factory.go:53` is a factory returning `outbound.BlockVerifier` with exactly one `kind` (`newEtherscanVerifier`) — a strategy selector with one strategy.
- `cmd/backfillers/morpho-vault-backfill` declares `countingMorphoRepository`, a **production** decorator over a 15-method port, living in a `cmd` package.
- 86 `postgres.New*` call sites across `cmd/` — worth cross-checking against whatever the composition-root/wiring investigation finds.
- The cache-key convention documented in `stl-verify/AGENTS.md` ("stl:{chainId}:{blockNumber}:{version}:{dataType}") is implemented independently in `adapters/outbound/redis/blockcache.go:286` and `internal/pkg/rawsckey` — two spellings of one format.
- `internal/pkg/blockchain` holds `AaveResolver` and `SparkLendResolver`, the only two-implementation seam other than `Multicaller`, yet neither lives in `adapters/outbound/` — the adapters/pkg boundary does not track the ports/adapters boundary.
- `stl-verify/AGENTS.md:207-220` ("Adding New Features": New Use Case / New External Dependency / New Entity) describes a workflow that no code in the repo follows — see F09.4.

## 5. Open questions

- **Do the k8s liveness/readiness probes target `/health/ready` and `/health/live`?** `internal/adapters/inbound/http/health.go` is the only thing in the Go service serving those paths and it has no production caller. Either the probes are failing/absent, or they hit a sidecar or another language's service. This gates the delete in F09.4 and needs a look at `k8s/`.
- `outbound.Repository.HealthCheck` and `inbound.VerificationService.Ping` may be intended for a not-yet-built API surface (the Python APIs?). Worth one question to a maintainer before deleting, though nothing in `stl-verify/` references them.
- Is `offchain_price.go:109 MarketCapUSD *float64` a deliberate exception to the `big.Int`-not-`float64` rule (a market cap is not a token amount), or an oversight? The doc comment does not say.
- `entity/maple` is the only per-context subpackage of `entity`. Was it a deliberate first step toward splitting the flat package (F09.5), or specific to maple's off-chain-API lineage? Its `doc.go` does not say which, but it does two other useful things: it describes the package as "registry rows and their per-cycle state snapshots" — the persistence model, in the domain's own words (F09.5) — and it claims "Like the parent entity package, it has no dependencies outside the standard library", which is **false for the parent**: `go list` reports `internal/domain/entity` importing `github.com/ethereum/go-ethereum/common` and `.../crypto` (15 files). `maple` itself is clean (`fmt`, `math/big`, `time` only).
- **Resolved, partially:** no ADR sanctions `pgx.Tx` in the ports layer. `docs/adr/` (5 ADRs) mentions pgx only in an example snippet (`0002:82,88`) and requires single-transaction semantics in prose (`0006:234` "in one transaction, serialised and idempotent"; `0006:1440`). That requirement is what the `tx` parameter is *for*, and an opaque `outbound.Tx` handle satisfies it identically — so F09.1 removes the driver type without touching the guarantee. Still worth one maintainer question in case the reasoning is unwritten.
