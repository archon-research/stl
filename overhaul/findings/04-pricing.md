Status: FINAL

# 04 — Prices, oracles, and the live-data / HTTP path

Area owner: investigation agent 04. All paths relative to repo root unless absolute.
`.claude/worktrees/` was excluded from every count (it is a full second copy of the tree).

## 1. Area map

Two independent price pipelines write two tables, and they share almost nothing.

**On-chain oracle prices → `onchain_token_price`.** `oracle` / `oracle_asset` registry rows are
the only configuration; there are no hard-coded feeds in Go. `oracle_pricing.LoadOracleUnits`
reads the registry (pinned to a `referenceEffectiveAt`, ADR-0006 §4) and builds one `OracleUnit`
per oracle. Two consumers then price blocks from those units: the live SQS worker
(`oracle_price_worker`, one message per block, reads the block payload from Redis→S3 for the
timestamp) and the one-shot CLI backfiller (`oracle_backfill`, worker pool over a block range,
reads timestamps from `HeaderByNumber`). Both call the same four fetchers in
`internal/pkg/blockchain` (`FetchOraclePrices`, `FetchFeedPrices`, `FetchERC4626SharePrices`,
`FetchCurveLPNGPrices`) through `outbound.Multicaller`, and both persist through
`outbound.OnchainPriceRepository`.

**Off-chain prices → `offchain_token_price`.** `offchain_price_fetcher.Service` sits behind
`outbound.PriceProvider` (one adapter: `coingecko`) and `outbound.PriceRepository`. Two
composition roots drive it: a Temporal cronjob (`cmd/cronjobs/offchain-price-indexer`, 5-minute
`FetchCurrentPrices`) and an on-demand Temporal worker (`cmd/backfillers/offchain-price-backfill`,
`BackfillChunk` per window).

**Not price code, despite the folder name.** `internal/services/live_data` is the *watcher's*
live block service (WebSocket headers, reorg detection, Redis cache write, SNS publish), wired
only from `cmd/base/watcher/main.go`. `internal/adapters/inbound/http` + `internal/ports/inbound`
are dead template scaffolding (F04.6). `sky` / `skydata` are risk-capital and balance-sheet
feeds, not price sources; they are in scope only because they share `pkg/httpclient` and
`pkg/skyenvelope`.

```text
oracle/oracle_asset (DB)
        |
        v
oracle_pricing.LoadOracleUnits ---> []*OracleUnit
        |                                  |
        v                                  v
oracle_price_worker.Service        oracle_backfill.Service
 (SQS, hash-pinned)                 (worker pool, number-pinned)
        \                                  /
         \--> internal/pkg/blockchain fetchers --> outbound.Multicaller
                          |
                          v
              outbound.OnchainPriceRepository --> onchain_token_price

coingecko.Client --> outbound.PriceProvider --> offchain_price_fetcher.Service
                                                   |            |
                                        cronjob (current)   Temporal (backfill)
                                                   \            /
                                        outbound.PriceRepository --> offchain_token_price
```

## 2. Metrics

| Package | .go files | src lines | test lines | test:src |
|---|---|---|---|---|
| `internal/services/oracle_price_worker` | 12 | 1092 | 6222 | 5.7 |
| `internal/services/oracle_backfill` | 5 | 702 | 4032 | 5.7 |
| `internal/services/oracle_pricing` | 3 | 436 | 1219 | 2.8 |
| `internal/services/offchain_price_fetcher` | 4 | 496 | 2291 | 4.6 |
| `internal/services/live_data` | 2 | 1163 | 3942 | 3.4 |
| `internal/adapters/outbound/coingecko` | 3 | 343 | 316 | 0.9 |
| `internal/adapters/outbound/sky` | 2 | 404 | 408 | 1.0 |
| `internal/adapters/outbound/skydata` | 2 | 343 | 423 | 1.2 |
| `internal/pkg/httpclient` | 2 | 202 | **27** | **0.13** |
| `internal/pkg/retry` | 2 | 145 | 280 | 1.9 |
| `internal/pkg/skyenvelope` | 2 | 56 | 52 | 0.9 |
| `internal/adapters/inbound/http` | 3 | 239 | 210 | 0.9 |
| `internal/ports/inbound` | 1 | 32 | 0 | — |
| `cmd/workers/oracle-price-indexer` | 3 | 277 | 616 | 2.2 |
| `cmd/backfillers/oracle-pricing-backfill` | 3 | 187 | 280 | 1.5 |
| `cmd/backfillers/offchain-price-backfill` | 4 | 486 | 809 | 1.7 |
| `cmd/cronjobs/offchain-price-indexer` | 1 | 95 | 0 | — |
| Pricing spine in `internal/pkg/blockchain` | 4 | 1011 | 1891 | 1.9 |
| Price repositories in `.../postgres` | 2 | 649 | (shared file) | — |

Largest source files: `live_data/live_data_service.go` 1163; `oracle_price_worker/service.go`
759; `oracle_backfill/service.go` 702; `postgres/onchain_price_repository.go` 455;
`oracle_pricing/oracle_unit.go` 436; `blockchain/feed_prices.go` 359.

Largest functions in area (lines):

| Lines | Location |
|---|---|
| 168 | `live_data/live_data_service.go:778` `handleReorg` |
| 144 | `cmd/workers/oracle-price-indexer/main.go:133` `run` |
| 115 | `live_data/live_data_service.go:1046` `cacheAndPublishBlockData` |
| 107 | `live_data/live_data_service.go:630` `detectReorg` |
| 97 | `blockchain/erc4626_share_prices.go:116` `ValidateERC4626UnderlyingDecimals` |
| 96 | `oracle_backfill/service.go:238` `runForOracle` |
| 93 | `cmd/backfillers/oracle-pricing-backfill/main.go:94` `run` |
| 90 | `live_data/live_data_service.go:406` `decideReorgAction` |
| 85 | `offchain_price_fetcher/service.go:140` `FetchHistoricalData` |
| 81 | `oracle_backfill/service.go:362` `worker` |

Hand-rolled test doubles in area: **18** types. Three of them (`mockRepo` in
`oracle_backfill/service_test.go:30`, `oracle_price_worker/service_test.go:130`,
`oracle_pricing/oracle_unit_test.go:25`) are near-copies of each other — 78 identical lines
between the latter two. `live_data_service_test.go` alone declares 9.

Ports consumed in area: `outbound.OnchainPriceRepository` (1 adapter), `outbound.PriceRepository`
(1 adapter), `outbound.PriceProvider` (1 adapter: coingecko), `outbound.Multicaller` (2 adapters:
`multicall.Client`, `multicall.DirectCaller` — a real seam), `outbound.SQSConsumer`,
`outbound.BlockCacheReader`, `outbound.RiskCapitalProvider`, `outbound.BalanceSheetProvider`.
Ports implemented in area: `inbound.VerificationService` (dead), `inbound.HealthChecker` (dead).

Churn since 2026-03-01 (`git log --oneline | wc -l`): `oracle_price_worker/service.go` 14,
`cmd/workers/oracle-price-indexer/main.go` 15, `postgres/onchain_price_repository.go` 7,
`oracle_backfill/service.go` 6, `oracle_pricing/oracle_unit.go` 5,
`live_data/live_data_service.go` 5.

## 3. Findings

### F04.1 — Adding one price source fans out to 9 type-dispatch sites across 3 packages and 2 mains

**Strength:** Strong
**Files:**
- `internal/services/oracle_pricing/oracle_unit.go:91-100` (`ValidationFeeds`), `:136-145` (`buildOracleUnit`)
- `internal/services/oracle_price_worker/service.go:74-78` (4 ABI fields), `:132-155` (4 `abis.Get*ABI` loads), `:259-309` (`logOracleUnit`), `:310-333` (`validateFeedDecimals`), `:428-438` (`processBlockForOracle`), `:442-585` (4 `processBlockFor*Oracle` methods)
- `internal/services/oracle_backfill/service.go:72-75`, `:115-133`, `:190`, `:398-409`, `:443-577`
- `cmd/workers/oracle-price-indexer/main.go:257-268` and `cmd/backfillers/oracle-pricing-backfill/main.go:146-158` (`RequiresDirectCall` multicaller routing, written twice)
- `internal/domain/entity/onchain_price.go:13-58` (enum + 4 predicate methods)

**Problem.** `OracleType` has six values and no polymorphism. `grep` for the enum/predicates
outside the entity file returns **21 branch lines forming 9 distinct dispatch points**: 7 inside the three service packages (backfill `:190`, `:398-409`; worker `:259-309`, `:322`, `:428-438`; `oracle_pricing` `:91-100`, `:136-145`) plus one in each main. Both
`Service` structs carry the same four `*abi.ABI` fields and repeat the same four `abis.Get*ABI()`
calls in their constructors. `MulticallerFactory` (worker `service.go:37`) and `MulticallFactory`
(backfill `service.go:36`) are the *same* type declared twice:
`func(entity.OracleType) (outbound.Multicaller, error)`. The evidence that this is the driver of
the repo's most-churned Go file: the two `NEW_ORACLE_TYPE` commits touched 21 files
(`a3273540`, erc4626) and 25 files (`4e82d2d2`, curve_lp_ng) each, and each had to edit *both*
services and (for erc4626) both mains. The two reorg-correctness sweeps (`c92be237` 74 files,
`9f92e344` 58 files) each had to edit all three per-type paths in the worker, because pinning is
per-fetch-signature. By contrast adding a *feed* is a one-file SQL migration (`0b27d620`,
`3b22f57d` are 1 file each) and adding a *chain* is one Go line plus nine k8s files.
`oracle_type` is not even CHECK-constrained (`db/migrations/20260709_120000_add_er_missing_price_feeds.sql:164`),
so the whole cost of a new source is Go fan-out.

**Proposed change.** Introduce one seam — an `OraclePricer` per oracle type — and register it once:

```go
// in oracle_pricing
type OraclePricer interface {
    Name() string                                   // for logs/metrics labels
    Describe(unit *OracleUnit) []slog.Attr          // replaces logOracleUnit's switch
    ValidationFeeds(unit *OracleUnit) []blockchain.FeedConfig
    ValidateConfig(ctx, mc, unit, blockNum) error   // absorbs the ERC4626 extra step
    Quote(ctx, mc, unit *OracleUnit, at BlockRef) ([]PriceQuote, error)
}
func PricerFor(t entity.OracleType) (OraclePricer, error)  // one registry map
```

`BlockRef` carries `{Number int64; Hash common.Hash}` so the hash-vs-number decision is data,
not a call-site convention (see F04.2). Each pricer owns its own ABIs, so the four ABI fields
leave both `Service` structs. Both services then become: load units → `PricerFor` → validate →
`Quote` → persist. `MulticallerFactory` collapses into the registry (`RequiresDirectCall` is a
property of the pricer), deleting the closure from both mains.

**Benefits.** Locality: a new price source is one new file plus one registry line, versus 8
edits in 3 packages today. Leverage: a correctness sweep like VEC-471 becomes one edit inside
`Quote`'s shared prologue instead of 4+3 per-type edits. Tests: each pricer is testable against a
fake `Multicaller` without an SQS consumer, a Redis cache reader, or a 12-method repo mock — the
7.3k lines of `oracle_price_worker` tests exist largely because the only way to reach a per-type
path today is through `Service.processBlock`.

**Risk / migration.** Land the registry with pricers that delegate to the existing
`internal/pkg/blockchain` functions first (pure move, tests unchanged), then move the ABI
ownership, then delete the switches. `BlockRef` should be added before the pricers so the
number/hash divergence is settled once. The `oracle.unit.*` metric labels must keep using
`oracle.Name`, not the pricer name, or the alerts in `alerts/vector-indexers.yaml:222-240` break.

**Size:** L (3–4 PRs)
**Enables:** F04.2, F04.4, F04.7

### F04.2 — `oracle_price_worker` and `oracle_backfill` are two copies of one pipeline that have silently diverged in five behaviours

**Strength:** Strong
**Files:** `internal/services/oracle_price_worker/service.go` (759) vs
`internal/services/oracle_backfill/service.go` (702)

**Problem.** Ten matched function pairs, same job, separate code:

| Worker | Backfill | Same job |
|---|---|---|
| `NewService` 105-171 | `NewService` 81-146 | nil-checks + 4 identical `abis.Get*ABI` loads |
| `validateFeedDecimals` 310-333 | `validateFeedDecimals` 174-204 | loop units → `ValidationFeeds` → `ValidateFeedDecimals` → ERC4626 extra |
| `processBlockForOracle` 417-441 | `worker` switch 398-409 | `OracleType` dispatch |
| `processBlockForAaveOracle` 442-510 | `processBlockAave` 443-502 | Aave path |
| `processBlockForFeedOracle` 511-536 | `processBlockFeed` 504-526 | feed path |
| `processBlockForERC4626Oracle` 537-558 | `processBlockERC4626` 528-548 | erc4626 path |
| `processBlockForCurveLPNGOracle` 559-585 | `processBlockCurveLPNG` 555-577 | curve path |
| `detectChanges` 635-682 | inline in `worker` 422-429 | change detection (aave) |
| `detectFeedChanges` 683-715 | inline in `worker` 422-429 | change detection (feed) |
| `storeFeedResults` 586-634 | `batchWriter` 633-658 | persist |

Plus verbatim duplicates: `priceDecimals == 0 → 8` (worker `:639`, backfill `:384`); the
`MulticallerFactory`/`MulticallFactory` type; `blockHash, err := event.ParsedBlockHash()` repeated
in the worker's four per-type methods (`:443`, `:512`, `:538`, `:560`). The parallel test suites
duplicate too: `service_curve_lp_ng_test.go` exists in both packages (578 and 463 lines).

The five divergences, none of them documented as a deliberate difference in behaviour:

1. **Aave reads differ in kind.** Worker uses `blockchain.FetchOraclePrices` — one batched
   `getAssetsPrices` with `AllowFailure: false`, so a revert fails the whole block
   (`oracle.go:50-65`). Backfill uses `FetchOraclePricesIndividual` — N `getAssetPrice` calls with
   `AllowFailure: true`, and unparseable results are `slog.Warn`-and-skipped inside the fetcher
   (`oracle.go:136-149`). The same oracle at the same block therefore yields "all-or-nothing" live
   and "whatever succeeded" in backfill.
2. **Pinning.** Worker hash-pins all four paths. Backfill number-pins feed and erc4626 (passes
   `common.Hash{}`, `service.go:516`, `:540`), but hash-pins curve because
   `FetchCurveLPNGPrices` refuses a zero hash (`curve_lp_prices.go:59-62`), and number-pins Aave
   because the individual fetcher has no hash variant.
3. **Change-detection commit point.** Worker caches only after `UpsertPrices` succeeds, with a
   comment explaining that caching earlier would let an SQS redelivery ack and drop rows forever
   (`service.go:44-49`, `commitPriceCache`). Backfill updates `prevPrices` *before* the batch
   writer has flushed (`service.go:422-429`).
4. **Telemetry.** Worker has 7 metrics and 4 alerts (`oracle_price_worker/telemetry.go`,
   `alerts/vector-indexers.yaml:111-240`). Backfill has none.
5. **Error policy.** See F04.3.

**Proposed change.** With F04.1's `OraclePricer` in place, extract the remaining shared tail into
`oracle_pricing`: a `PriceRun` that takes `{unit, pricer, BlockRef, timestampSource, sink}` and
owns validate → quote → change-detect → persist → commit-cache. The worker's sink is
"upsert then commit cache"; the backfiller's sink is "append to the batch channel". Give the
backfiller `BlockRef{Number, Hash}` from `HeaderByNumber` (it already fetches the header for
curve and for every timestamp) so pinning stops being a per-path decision, and delete
`FetchOraclePricesIndividual` in favour of the batched call plus an explicit
"oracle not yet configured at this block" pre-check.

**Benefits.** Locality: one implementation of "price this oracle at this block". Leverage: the
backfiller inherits the worker's telemetry and its cache-commit ordering for free. Tests: the
~10.2k lines of tests across the two packages collapse toward one suite for `PriceRun` plus a
per-pricer suite.

**Risk / migration.** The Aave unification changes backfill behaviour at blocks where the oracle
exists but an asset has no source — today those are silently skipped. That needs a deliberate
decision (probably: clamp the range per asset from the registry, then fail hard), and it is the
riskiest step; do it last and separately.

**Size:** L
**Depends on:** F04.1

### F04.3 — `oracle_backfill` swallows every per-block failure and exits 0

**Strength:** Strong
**Files:** `internal/services/oracle_backfill/service.go:362-441` (`worker`), `:238-326`
(`runForOracle`), `:149-172` (`Run`); `cmd/backfillers/oracle-pricing-backfill/main.go:30-36`

**Problem.** In `worker`, a failed block is counted and skipped:

```go
if blockErr != nil {
    s.logger.Error("failed to process block", "block", blockNum, "error", blockErr)
    stats.blocksFailed.Add(1)
    continue
}
```

and a failed multicaller construction drops the worker's whole sub-range:

```go
stats.blocksFailed.Add(rangeSize)
s.logger.Error("failed to create multicall client", ..., "blocksDropped", rangeSize)
return
```

`runForOracle` then logs `"backfill complete"` with the `errors` count and returns `nil`; `Run`
returns `nil`; `main` prints `"completed successfully"` and exits 0. A run in which *every* block
failed is indistinguishable from a clean one to any caller, CI check, or operator reading the exit
code. This is the exact failure mode `stl-verify/AGENTS.md` forbids ("Never swallow a failure into
partial success… Silent partial data is the worst outcome: it looks healthy, and repairing the
holes later forces a backfiller rerun"), and it is the pipeline whose whole purpose is repairing
holes. `internal/pkg/blockchain/erc4626_share_prices.go:80-86` even documents the behaviour as
intended ("the backfill service logs the block as failed and continues (it does not abort the
run)") — intent recorded, not justified.

By contrast `offchain_price_fetcher.FetchHistoricalData` (`service.go:205-207`) collects
`failedAssets` and returns an aggregate error, and the Temporal workflow has three separate
coverage assertions (`cmd/backfillers/offchain-price-backfill/backfill.go:239-260`). The two
backfillers apply opposite policies to the same class of failure.

**Proposed change.** Make the run's verdict a value, not a log line. `Run` returns a
`BackfillReport{BlocksProcessed, BlocksFailed, FirstError}` and a non-nil error whenever
`BlocksFailed > 0`; `main` prints the report and exits non-zero. Keep the worker pool
fail-soft *within* a run so one bad block does not abort 10M others, but record the failed block
numbers (bounded, e.g. first 100 plus a count) so the report names the holes to re-run. A
multicaller-construction failure is a startup fault, not a per-range one — build one multicaller
per oracle type before the pool starts and fail `Run`.

**Benefits.** Locality: one place decides what "a backfill succeeded" means. Leverage: the exit
code becomes usable by the runbooks and by any future automation. Tests: assertions move from
"check the log output" to "check the returned report".

**Risk / migration.** Anyone currently running the binary over a range that contains
pre-deployment blocks will start seeing a non-zero exit; that is the point, but the per-oracle
`validFrom` clamping (`service.go:334-353`) must be verified to cover the legitimate cases first.

**Size:** M
**Depends on:** nothing (can land immediately, independent of F04.1/F04.2)

### F04.4 — "A non-positive value is not a price" is enforced at 8 sites with 3 different verdicts, and the entity permits zero

**Strength:** Strong
**Files:**
- `internal/domain/entity/onchain_price.go:112-131` — `Validate` rejects only `PriceUSD < 0`
- `internal/pkg/blockchain/oracle.go:164-171` — `ScaleByDecimals` returns `0` for a nil or zero input
- `internal/pkg/blockchain/feed_prices.go:159-165` — non-positive → retry with `latestAnswer`
- `internal/pkg/blockchain/feed_prices.go:226-231` — non-positive → warn, leave `Success=false`
- `internal/pkg/blockchain/erc4626_share_prices.go:275-288` — non-positive → warn, soft skip; `:80-86` all-failed → hard error
- `internal/pkg/blockchain/curve_lp_prices.go:134-137`, `:155-158` — non-positive → **hard error**, fails the block
- `internal/services/oracle_pricing/oracle_unit.go:110-127` — missing reference feed → set `Price = 0; Success = false`
- `internal/services/oracle_price_worker/service.go:643-656` — nil/zero → warn and skip; `:716-728` `countNonZeroPrices` re-derives the same predicate for the metric

**Problem.** One fact ("this oracle cannot price this token at this block") gets four different
answers depending on which oracle type you happen to be reading: curve fails the block, erc4626
soft-skips unless *all* vaults failed, feed retries with a different method then skips, aave
skips with a warning. The comments at `service.go:643-656` and `:716-728` exist only to keep the
metric's predicate in sync with the skip predicate by hand. And because
`entity.OnchainTokenPrice.Validate` allows `0`, nothing structurally prevents a `$0` row: any
future path that builds a `FeedPriceResult{Success: true, Price: 0}` will persist it, and
`ScaleByDecimals` manufactures exactly that value from a nil `*big.Int`.

**Proposed change.** Give the domain a `PriceQuote` type that cannot hold a non-price:

```go
type PriceQuote struct { TokenID int64; priceUSD decimal }   // constructor rejects <= 0
func NewPriceQuote(tokenID int64, raw *big.Int, decimals int) (PriceQuote, bool, error)
```

`ok == false` means "unpriceable at this block" — a structural fact every caller must handle —
and `err` means a real fault. Make `entity.OnchainTokenPrice.Validate` reject `PriceUSD <= 0`,
make `ScaleByDecimals` return `(float64, error)` (or delete it in favour of the constructor), and
pick one documented verdict for "unpriceable": propagate an error naming the token and block, and
require the pricer to *not issue* the call for tokens that are structurally unpriceable at that
block — which is what `stl-verify/AGENTS.md` already mandates ("gate it structurally").

**Benefits.** Locality: the invariant lives on the type, so the metric predicate, the skip
predicate and the DB constraint stop being three hand-synced copies. Leverage: a new oracle type
gets the policy for free instead of choosing one of four. Tests: the "zero price" edge cases
currently spread across four `_test.go` files become one table test on the constructor.

**Risk / migration.** Zero rows may already exist in `onchain_token_price`; a `CHECK` on the
column needs a data audit first (the append-only rule in `db/migrations/AGENTS.md` means you
cannot fix them in place). Land the Go-side constructor and the `Validate` tightening first, add
the constraint in a later migration.

**Size:** M
**Enables:** F04.1 (the pricer's `Quote` returns `[]PriceQuote`)

### F04.5 — `outbound.OnchainPriceRepository` has 12 methods, 6 with no production caller, and 3 hand-rolled mocks that must implement all 12

**Strength:** Strong
**Files:** `internal/ports/outbound/onchain_price_repository.go:18-63`;
`internal/services/oracle_backfill/service_test.go:30-147`;
`internal/services/oracle_price_worker/service_test.go:130-241`;
`internal/services/oracle_pricing/oracle_unit_test.go:25-119`;
`internal/adapters/outbound/postgres/onchain_price_repository.go`

**Problem.** Consumers by method, excluding tests and the adapter itself:

| Method | Production callers |
|---|---|
| `UpsertPrices` | worker, backfill, (offchain fetcher via its own port) |
| `GetEnabledAssets`, `GetTokenInfos`, `GetEnabledOraclesByChain` | `oracle_pricing` only |
| `GetLatestPrices` | worker only |
| `GetAllProtocolOracleBindings` | backfill only |
| `GetOracle`, `GetLatestBlock`, `GetOracleByAddress`, `InsertOracle`, `InsertProtocolOracleBinding`, `CopyOracleAssets` | **none** |

Those last six appear in exactly three kinds of place: the port, the postgres adapter, and a stub
method on each of the three `mockRepo` copies. That is 18 stub methods written to satisfy an
interface no production code uses. The three `mockRepo` types declare the same twelve `*Fn`
fields in the same order; 78 lines are byte-identical between the `oracle_price_worker` and
`oracle_pricing` copies. Adding one repository method today means editing the port, the adapter,
and three mocks — five files for one query, and the fan-out is the reason
`onchain_price_repository.go` shows 7 commits since March.

`CopyOracleAssets` is the interesting one: it has 8 references in
`onchain_price_repository_integration_test.go` and none anywhere else, i.e. it is a method that
exists to be tested.

**Proposed change.** Split the port along its actual consumers and delete the orphans:

- `OracleRegistryReader` — `GetEnabledOraclesByChain`, `GetEnabledAssets`, `GetTokenInfos`
  (consumed by `oracle_pricing` alone)
- `OnchainPriceWriter` — `UpsertPrices`
- `OnchainPriceReader` — `GetLatestPrices`
- `ProtocolOracleBindingReader` — `GetAllProtocolOracleBindings`

`*postgres.OnchainPriceRepository` keeps satisfying all four. Delete `GetOracle`,
`GetLatestBlock`, `GetOracleByAddress`, `InsertOracle`, `InsertProtocolOracleBinding` and
`CopyOracleAssets` from the port; keep them as adapter methods only if the integration tests
genuinely cover a migration path, otherwise delete them too. Then replace the three `mockRepo`
copies with one fixture-factory double in a shared `oracle_pricing/pricingtest` (or `testutil`)
package — three small interfaces need three small doubles, and a factory with sensible defaults
covers the varying cases per `AGENTS.md`'s "fixture factories for varying data".

**Benefits.** Locality: `oracle_pricing`'s tests stop compiling against methods it never calls.
Leverage: adding a query touches the port it belongs to plus the adapter — two files. Tests: the
three near-identical doubles become one, and the 18 dead stubs go.

**Risk / migration.** Purely mechanical; the compile-time assertion at
`postgres/onchain_price_repository.go:23` catches any miss. Do the deletions in a separate PR from
the split so the diff stays readable.

**Size:** M
**Depends on:** none

### F04.6 — The entire inbound side of the hexagon is dead scaffolding, and its doc comment names an implementation that does not exist

**Strength:** Strong
**Files:** `internal/ports/inbound/services.go` (32); `internal/adapters/inbound/http/handler.go`
(57), `health.go` (182), `health_test.go` (210); `internal/services/verification_service.go` (30)

**Problem.** `go list -f '{{.ImportPath}}: {{join .Imports " "}}' ./...` returns **no package in
the module** importing `internal/adapters/inbound/http`. `internal/ports/inbound` is imported only
by that dead package and by `verification_service.go`, and nothing imports the `internal/services`
root package either. Concretely:

- `inbound.VerificationService` (`services.go:9-14`) is a template stub — its body is
  `// Add your use case methods here` plus `Ping`. Its only implementation,
  `services.VerificationService`, has zero callers including tests.
- `NewHandler` (`handler.go:22`) has zero callers. `RegisterRoutes` ends with
  `// Add more routes as needed`.
- `NewHealthServer` (`health.go:65`) is called only from `health_test.go` (3 times). The real
  liveness probe is `exec: ["/bin/sh","-c","pgrep -f watcher"]`
  (`k8s/base/watcher/deployment.yaml:45-49`) — no HTTP health endpoint is used anywhere; only
  `k8s/base/python-api/deployment.yaml` has an HTTP probe.
- `inbound.HealthChecker`'s doc comment says "Implementations: LiveService: Ready after first
  block processed" (`services.go:20-22`). `grep -rn "IsReady\|IsHealthy" internal/services/`
  returns nothing. The port has one adapter and it is a test double.

This is ~511 source lines (plus 210 test lines) of Ports-and-Adapters skeleton that documents an
architecture the service does not have, and it is the first thing a reader of
`stl-verify/AGENTS.md`'s "New Use Case: 1. Add method to inbound port… 3. Add HTTP handler" will
find and imitate.

**Proposed change.** Delete `internal/ports/inbound`, `internal/adapters/inbound/http`, and
`internal/services/verification_service.go`. Move
`internal/services/services_integration_test.go` (the one real inhabitant of the `services` root
package, a live+backfill concurrency scenario) to `internal/services/scenario/` or into
`live_data`. Update `stl-verify/AGENTS.md`'s architecture tree and its "New Use Case" recipe to
describe what the service actually is: a fleet of SQS/Temporal workers with no inbound HTTP
surface. If an HTTP health endpoint is genuinely wanted, that is a separate, deliberate change
that starts with a k8s probe.

**Benefits.** Locality: one fewer architecture to keep in your head. Leverage: removes a
misleading template that the AGENTS.md actively points new work at. Deletion test: nothing
reappears anywhere — the complexity is zero.

**Risk / migration.** None beyond the test move; the compiler proves the rest.

**Size:** S
**Depends on:** none

### F04.7 — The pricing spine hand-rolls the multicall pack/execute/count-check/unpack skeleton 8 times, while `shared.RunSnapshotReads` already exists

**Strength:** Strong
**Files:**
- Skeleton: `internal/pkg/blockchain/oracle.go:44-72`, `:107-151`;
  `feed_prices.go:120-171`, `:192-240`, `:289-346`;
  `erc4626_share_prices.go:50-77`, `:130-180` (two batches);
  `curve_lp_prices.go:56-82`
- Bespoke unpackers: `oracle.go:75-81`, `:154-160`; `feed_prices.go:243-253`, `:255-266`,
  `:349-359`; `erc4626_share_prices.go:294-303`; `curve_lp_prices.go:166-176`
- Already-existing helpers: `internal/services/shared/snapshotread.go:22-69`
  (`SnapshotRead[P]` + `RunSnapshotReads`), `internal/services/shared/abidecode.go:20-40`
  (`UnpackUint`)

**Problem.** Every price fetcher repeats: `ABI.Pack` → build `[]outbound.Call` with an
`AllowFailure` choice → `ExecutePinned` → `if len(results) != len(calls) { error }` → iterate by
index and unpack-or-skip. `grep "results, got %d"` finds **24 such count-checks repo-wide, 8 of
them in the four pricing files**. Seven of the unpackers are the same five lines with a different
method name; four of them (`unpackAssetPrice`, `unpackConvertToAssets`, `unpackVirtualPrice`, and
the `*big.Int` half of `unpackLatestAnswer`) are literally `shared.UnpackUint` with the method
string changed — and `shared.UnpackUint`'s doc even says it is "the shared implementation used by
the per-DEX multicall readers", i.e. the DEX indexers already solved this and the pricing spine
never adopted it. `shared.RunSnapshotReads` goes further: it owns the offset arithmetic that
`erc4626_share_prices.go:68` does by hand as `results[2*i]` / `results[2*i+1]`, which is exactly
the "hand-maintained positional cursor" its doc comment says it exists to remove.

**Proposed change.** Adopt `shared.SnapshotRead`/`RunSnapshotReads` in the four pricing fetchers,
after adding a pinned variant so it can serve the backfiller:

```go
func RunSnapshotReadsPinned[P any](ctx, mc, pool P, at BlockRef, reads []SnapshotRead[P]) error
```

(`RunSnapshotReads` today only calls `ExecuteAtHash`, `snapshotread.go:51`.) Replace the seven
unpackers with `shared.UnpackUint` plus one `unpackLatestRoundDataAnswer` that keeps the
"answer is field 1 of 5" knowledge. `ExecutePinned` (`oracle.go:25-30`) then becomes the internal
detail of the pinned runner rather than a convention 6 call sites must remember.

**Benefits.** Locality: the count-check invariant and the offset arithmetic live in one place that
already has a test suite. Leverage: the erc4626 two-call-per-vault layout stops being index
arithmetic in the caller. Tests: the "wrong result count" and "reverted sub-call" cases collapse
from 8 near-duplicate tests to one.

**Risk / migration.** `RunSnapshotReads` currently returns on the first `Decode` error, whereas
`FetchFeedPrices` deliberately continues past individual reverts and retries them with
`latestAnswer` (`feed_prices.go:73-82`). Model that as two reads (round-data, then a conditional
latest-answer read that packs zero calls when nothing failed — which `SnapshotRead`'s doc already
permits) rather than weakening the runner.

**Size:** M
**Depends on:** F04.1 for the `BlockRef` type (or introduce `BlockRef` here and let F04.1 consume it)

### F04.8 — Oracle prices round-trip through `float64` into a `NUMERIC(30,18)` column, and change detection compares the floats

**Strength:** Worth exploring
**Files:** `internal/pkg/blockchain/oracle.go:162-171` (`ScaleByDecimals`);
`internal/domain/entity/onchain_price.go:88-94` (`PriceUSD float64`);
`internal/services/oracle_price_worker/service.go:62-70` (`suppressAsUnchanged`: `cached == priceUSD`);
`internal/services/oracle_backfill/service.go:424` (`prev == p.PriceUSD`);
`db/migrations/20260206_100000_create_onchain_prices.sql:109` (`price_usd NUMERIC(30, 18)`)

**Problem.** The on-chain value arrives as an exact `*big.Int`. `ScaleByDecimals` builds a
`big.Float`, divides, and immediately calls `.Float64()`. The column that receives it holds 18
decimal places; `float64` carries ~15–17 significant decimal digits, so an 18-decimal Chronicle
or Curve feed loses its tail before it reaches Postgres, and the loss is silent. Two consequences
beyond precision: change detection is float equality on both paths, so a price change below
`float64` resolution is invisible and no row is written; and `stl-verify/AGENTS.md` states
"Amounts: Wei / token amounts are `big.Int`, never `float64`" — a USD price is an amount, and the
whole pricing spine is the exception.

**Proposed change.** Keep the exact value all the way to the driver: have the quote constructor
(F04.4) hold the raw `*big.Int` plus its decimals, expose a `String()` in fixed-point form, and
let pgx write that into `NUMERIC`. Change detection then compares the raw integers (exact, and
cheaper). `float64` stays only where a float is genuinely the source: the CoinGecko path, whose
JSON is a float already.

**Benefits.** Locality: one type owns "a price and its scale". Leverage: change detection becomes
exact, so no real oracle update is dropped. Tests: precision cases become assertable instead of
being written as tolerance comparisons.

**Risk / migration.** Needs a check that pgx and the read models (`transformed.onchain_token_price`,
`token_price_current`, the Python risk models) tolerate a string-encoded numeric. Measure whether
any live feed actually exceeds `float64` precision before spending the effort — this may turn out
to be theoretical, which is why it is Worth exploring rather than Strong.

**Size:** M
**Depends on:** F04.4

### F04.9 — Two chunk loops for CoinGecko history; only the Temporal one has the window-seam fix

**Strength:** Strong
**Files:** `internal/services/offchain_price_fetcher/service.go:275-296`
(`fetchHistoricalDataForAsset`) vs `cmd/backfillers/offchain-price-backfill/backfill.go:303-325`
(`chunkWindows`)

**Problem.** Both walk a range in `HistoricalChunkWidth` steps. The Temporal one advances
`start = end.Add(time.Second)` with a nine-line comment explaining why: CoinGecko's range is
inclusive at *both* ends, so abutting windows fetch and count the seam hour twice. The service
one advances `chunkStart = chunkEnd`, i.e. it has exactly the bug the comment describes. It is not
a data-corruption bug — the upsert is `ON CONFLICT DO NOTHING` — but the scheduled/CLI path does
duplicate work every 30 days of range and the two loops disagree about the provider's contract.
The rule is also absent from `docs/backfilling-offchain-prices.md`, so the only record of it is a
comment in the copy that got fixed.

**Proposed change.** Export `chunkWindows` from `offchain_price_fetcher` (it is pure and
deterministic, which is why it is safe in workflow code) and have both callers use it.
`fetchHistoricalDataForAsset` becomes a loop over `chunkWindows` calling `fetchAndStoreChunk`.
Note the seam rule once, in the exported function's doc.

**Benefits.** Locality: one place knows how wide a window may be and where windows meet. Leverage:
the next provider quirk is fixed once. Tests: the existing `chunkWindows` table test covers both
callers.

**Risk / migration.** `chunkWindows` currently takes `BackfillParams`; give it
`(assets []string, from, to time.Time)` so the `cmd` type does not leak into the service.

**Size:** S
**Depends on:** none

### F04.10 — Two composition roots build the off-chain price fetcher with ~33 identical lines

**Strength:** Strong
**Files:** `cmd/cronjobs/offchain-price-indexer/main.go:52-89` (`setupRunner`) vs
`cmd/backfillers/offchain-price-backfill/main.go:87-120` (`newPriceFetcher`)

**Problem.** Same sequence, same order, same error strings: `chainutil.RequireChainID` →
`env.Require("COINGECKO_API_KEY")` → `buildregistry.New(ctx, deps.Pool)` →
`coingecko.NewClient{APIKey, BaseURL: os.Getenv("COINGECKO_BASE_URL"), Logger}` →
`postgres.NewPriceRepository(deps.Pool, deps.Logger, buildReg.BuildID(), 0)` →
`offchain_price_fetcher.NewService`. The only differences: the cronjob sets `Concurrency: 5`
(the backfiller relies on the default 5 from `service.go:85-88` — same value, expressed twice) and
it wraps the result in a `temporal.RunnerFunc`. Both also repeat the same
`postgres.PoolOpener(postgres.DefaultDBConfig(env.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable")))`
literal, once inline and once via a `defaultDatabaseURL` constant.

**Proposed change.** One exported constructor in `offchain_price_fetcher` (or a small
`pricingwire` package next to `archivingwire`):
`func FromEnv(ctx context.Context, deps temporal.Dependencies) (*Service, error)`. Both mains call
it. Move the default DSN into `postgres` so it is not written twice.

**Benefits.** Locality: the env-var contract for the off-chain price path lives in one function.
Leverage: adding a second `PriceProvider` adapter is one edit, not two. Tests: the wiring becomes
unit-testable instead of only reachable through two `main_integration_test.go` files.

**Risk / migration.** Trivial; both call sites are covered by integration tests today.

**Size:** S
**Depends on:** none

### F04.11 — `sky` and `skydata` duplicate `networkToChainID` + `chainIDFor` verbatim, on top of two more chain-name maps in `entity`

**Strength:** Worth exploring
**Files:** `internal/adapters/outbound/sky/risk_capital_client.go:34-47`, `:345-353`;
`internal/adapters/outbound/skydata/balance_sheet_client.go:33-45`, `:301-309`;
`internal/domain/entity/chain.go:36-45` (`ChainIDToName`), `:60-70` (`ChainIDToS3Bucket`)

**Problem.** The 6-entry `networkToChainID` map and the 9-line `chainIDFor` helper are
byte-identical in both adapters, including the comment. Both comments justify the duplication
("two vendors' vocabularies that happen to agree today"), which is a defensible argument for two
*maps* — but not for two copies of the lookup function, and not for the fact that the repo now
carries four overlapping chain-name tables that spell the same chains differently
(`entity.ChainIDToName` says `mainnet` and `avalanche-c`; `ChainIDToS3Bucket` says `ethereum` and
`avalanche`; both vendor maps say `ethereum` and `avalanche`). A fifth spelling lives in
`internal/services/allocation_tracker/chains.go:54`.

**Proposed change.** Keep two per-vendor maps (the stated reason is sound) but move the lookup to
one generic helper — `chainutil.LookupFolded(m map[string]int64, name string) *int64` — and add a
table test that asserts each vendor map against `entity.ChainIDToName` so a divergence is a
deliberate, visible edit rather than a silent one. Separately, `entity`'s two maps and the
allocation-tracker set want a single chain registry with named spellings per context; that is
bigger than this area.

**Benefits.** Locality: one folded-lookup implementation. Leverage: a new chain is one row per
vendor with a test that catches an omission.

**Risk / migration.** None; the maps stay per-vendor, so the documented concern is preserved.

**Size:** S

### F04.12 — `coingecko.ClientConfig` is a shallow re-declaration of `httpclient.Config`, and three adapters use three different defaulting idioms

**Strength:** Worth exploring
**Files:** `internal/adapters/outbound/coingecko/client.go:27-143`;
`internal/adapters/outbound/sky/risk_capital_client.go:56-107`;
`internal/adapters/outbound/skydata/balance_sheet_client.go:54-105`;
`internal/pkg/httpclient/client.go:17-39`

**Problem.** All three off-chain adapters do share `pkg/httpclient` and therefore `pkg/retry` and
`golang.org/x/time/rate` — the probe's worry about rolled-own HTTP/retry/rate-limiting is
unfounded. What they do not share is the config surface. `coingecko.ClientConfig` re-declares six
of `httpclient.Config`'s seven fields (`Timeout`, `MaxRetries`, `InitialBackoff`, `MaxBackoff`,
`BackoffFactor`, plus `RateLimitPerMin` as a different unit), adds a 26-line `applyDefaults`
that zero-checks each one (`client.go:118-143`), then copies them field-by-field into
`httpclient.Config` (`:100-108`). It also declares an `HTTPClient *http.Client` field that is
never read. `sky` re-declares five of them and applies them with a five-branch `if cfg.X > 0`
chain (`:84-99`). `skydata` declares only `Timeout` and overrides that one field. Three idioms,
one job. Apply the deletion test to `coingecko.ClientConfig`: remove it and the complexity moves
into `httpclient.Config` — where it already exists — so it is a pass-through.

**Proposed change.** Let each adapter's config embed or carry an `httpclient.Config` directly and
delete the mirrored fields, the `applyDefaults` copy, and the unused `HTTPClient` field. Keep only
genuinely adapter-specific fields (`APIKey`, `BaseURL`, `RateLimitPerMin` if the per-minute unit
is worth keeping, `Now` on skydata). Add `httpclient.Config.WithRateLimitPerMinute(n)` so the
rps/burst arithmetic at `coingecko/client.go:97-98` lives once.

**Benefits.** Locality: HTTP tuning is described in one struct. Tests: the defaulting behaviour is
tested once in `httpclient` instead of three times per adapter.

**Size:** S

### F04.13 — `pkg/httpclient` carries every off-chain feed and has 27 lines of tests, all on a private helper

**Strength:** Worth exploring
**Files:** `internal/pkg/httpclient/client.go` (202 src); `internal/pkg/httpclient/client_test.go`
(27); consumers `coingecko`, `sky`, `skydata`, `maple`, `etherscan`

**Problem.** Five adapters route every external request through `httpclient.Client`. Its
behaviour is load-bearing and non-obvious: 429 is retryable and bypasses the error parser
(`client.go:137-139`), 5xx is retryable without consulting the parser (`:141-143`), 4xx is
wrapped non-retryable (`:150-155`), a parser error on a *2xx* body is retryable (`:157-161`), and
the rate limiter's own error is non-retryable (`:109-111`). None of that is directly tested. The
single test asserts `buildRetryConfig` field mapping — a private function, which also contradicts
`AGENTS.md`'s "in services, ONLY test the public api". The behaviour is instead exercised
indirectly through **17 hand-rolled `httptest.NewServer` handlers across the 5 adapter test
files** (coingecko 4, etherscan 8, skydata 2, maple 2, sky 1), so each adapter re-derives the same
"429 then success", "500 then success", "malformed JSON" scenarios.

**Proposed change.** Add a table-driven `TestDoRequest_*` suite against `httptest` covering the
five status classes plus rate-limiter cancellation, and publish a small
`httpclienttest.NewServer(t, responses ...Response)` helper the adapters use in place of their own
handlers. Delete the `buildRetryConfig` test once the jitter assertion is covered through the
public path.

**Benefits.** Leverage: the retry/classification contract every feed depends on gets one
authoritative test. Tests: the adapter suites shrink to the parts that are actually
adapter-specific (URL shape, response mapping, error parser).

**Size:** S

### F04.14 — `live_data` is the watcher, not a price service; it is live, large, and misfiled

**Strength:** Strong (as a factual correction; the refactor itself is Worth exploring)
**Files:** `internal/services/live_data/live_data_service.go` (1163);
`cmd/base/watcher/main.go:35`, `:428-482`; `k8s/base/watcher/deployment.yaml`;
`stl-verify/Makefile:120-122`

**Problem.** The probe asked whether `live_data` is dead. It is not: `NewLiveService` is
constructed in `cmd/base/watcher/main.go:429`, the watcher has a k8s deployment and a
`make run-watcher` target, and `live_data` is additionally imported by
`internal/services/services_integration_test.go` and `oracle_price_worker/e2e_integration_test.go`.
It has nothing to do with prices. Its content is the block pipeline:

| Lines | Function | Job |
|---|---|---|
| 147-181 | `Start` / `Stop` | subscribe / drain |
| 182-245 | `processHeaders`, `startPrefetch` | header loop + block-body prefetch |
| 246-338 | `processBlockWithPrefetch`, `startProcessBlockSpan` | per-block orchestration |
| 339-405 | `runStateOps` | duplicate check → reorg decision → persist |
| 406-541 | `decideReorgAction` + 4 helpers | reorg verdict |
| 542-629 | `buildBlockState`, `persistBlockState`, `awaitPrefetch`, `isDuplicateBlock` | state write |
| 630-990 | `detectReorg`, `classifyOutOfOrderArrival`, `handleReorg`, `verifyIncomingIsCanonical` | **361 lines of reorg logic** |
| 991-1163 | `publishBlockEvent`, `cacheAndPublishBlockData`, `normalizeHash` | Redis write + SNS publish |

Three of the area's four largest functions are here (`handleReorg` 168, `cacheAndPublishBlockData`
115, `detectReorg` 107), and the reorg cluster returns
`(bool, int, int64, *outbound.ReorgEvent, error)` from two different functions — a 5-value
signature that is the reason `runStateOps` and `decideReorgAction` exist as intermediate
unpackers. Its test file (3942 lines) declares 9 hand-rolled doubles.

**Proposed change.** Two things, both for the watcher/reorg area rather than pricing: (a) extract
the 361-line reorg cluster into its own module with a named result type
(`ReorgVerdict{Kind, Depth, OrphanedFrom, Event}`) replacing the 5-tuple, which is what would let
`decideReorgAction` shrink to a switch; (b) rename the package to say what it is
(`blockwatcher` / `live_blocks`) and split `live_data_service.go` per the AGENTS.md file-pairing
rule — 1163 lines in one file with a single 3942-line `_test.go` violates it as written.

**Benefits.** Locality: reorg correctness — the single most consequential invariant in the repo —
gets one home instead of being spread across four methods of the watcher service. Tests: the
verdict type is testable without a cache, an event sink, or a state repo.

**Risk / migration.** High-consequence code; the extraction must be behaviour-preserving and the
existing 3942-line suite is the safety net. Do it as a pure move first.

**Size:** L
**Note:** Belongs to the watcher/reorg area, not pricing. Flagged here because it was assigned to
this area and the "possibly dead" hypothesis is wrong.

### F04.15 — The off-chain price path has no telemetry and no coverage check on the scheduled sweep, while the on-chain path has 7 metrics and 4 alerts

**Strength:** Worth exploring
**Files:** `internal/services/offchain_price_fetcher/service.go:101-135` (`FetchCurrentPrices`);
`internal/services/oracle_price_worker/telemetry.go` (333);
`alerts/vector-indexers.yaml:111-240`; `alerts/vector-cronjobs.yaml:195-203`

**Problem.** `grep` for `pkg/telemetry|otel` in `offchain_price_fetcher`, `coingecko` and
`oracle_backfill` returns nothing: only the live oracle worker is instrumented. Worse,
`FetchCurrentPrices` never compares what came back against what it asked for. CoinGecko's
`/simple/price` omits unknown IDs from a 200 response, and `convertToTokenPrices`
(`service.go:402-431`) only errors on an *extra* asset, never a missing one — so an asset that
stops being served is logged as `"stored current prices" count=N-1` every 5 minutes forever. The
same service's manual-backfill paths have three separate coverage assertions
(`assertRequestedAssetsResolved`, the `chunks > 0 && stored == 0` check at `service.go:304-309`,
and the workflow's `assertCoverage`) — the elaborate machinery is on the hand-run path and absent
from the automated one. `alerts/vector-cronjobs.yaml` only alerts on Temporal workflow failure,
which this is not.

**Proposed change.** Assert coverage in `FetchCurrentPrices`: every resolved asset with a
`token_id` must appear in the provider response, or the sweep fails (which the cronjob's existing
failure alert then catches). Add a per-source freshness gauge mirroring
`oracle.unit.last_success_timestamp_seconds` and the matching staleness rule, so a single asset
going dark is visible — the exact gap that `alerts/vector-indexers.yaml:222-240` was added to
close on the on-chain side.

**Benefits.** Locality: one definition of "the sweep succeeded". Leverage: the alert rule already
exists in shape; this reuses it. Per `alerts/AGENTS.md` and `docs/runbooks/AGENTS.md`, a new
indexer's definition of done includes both — this one predates that.

**Size:** S

### F04.16 — `oracle-price-indexer/main.go`'s churn is fleet bootstrap duplication, not oracle complexity

**Strength:** Strong
**Files:** `cmd/workers/oracle-price-indexer/main.go:70-131` (`parseConfig`, 63 lines),
`:133-277` (`run`, 144 lines); 11 sibling directories under `cmd/workers/`

**Problem.** 9 of this file's 15 commits since March are fleet-wide sweeps that edited 4–26
sibling `main.go` files in the same commit: `aba593a4` (24 siblings, path move), `91773d5e` (26,
lint), `5c8566bd` (14, SQS shutdown), `ac662cd3` (11, build registry), `0dd8b1c7` (7, DB error
metric), `58f9c196` (8 — its own subject reads "fix worker bootstrap drift across the 7
hand-rolled SQS worker mains"), `4e5dc064` (4), `b44e0b47` (5), `17b08499` (3). Only 2 of 15
(`a3273540`, `9f92e344`) are oracle-specific, and both are constructor-signature plumbing. The
144-line `run` is a linear script of 13 dependency constructions (OTEL, AWS config, SQS consumer,
Redis cache, S3 reader, cache-with-fallback, eth client, pg pool, build registry, chain name,
multicall telemetry, multicall client, oracle telemetry, archiving) — a body that
`AGENTS.md`'s function-composition rule would reject if it were new.

**Proposed change.** A `workerboot` package (or an extension of the existing
`internal/pkg/lifecycle`) that returns the standard bundle — logger, OTEL shutdown, AWS config,
SQS consumer, Redis+S3 cache reader, pg pool, build registry, chain name, multicall client and
telemetry, archiving wrap and drain — from one `Bootstrap(ctx, Spec{ServiceName, ChainID, ...})`
call, with the env/flag contract declared once. Each worker's `run` then reduces to
`boot → NewService → lifecycle.RunWithTimeoutGuard`. Note this is cross-area: the payoff is 12
workers, not one.

**Benefits.** Locality: the next fleet sweep is one file instead of 14–26. Leverage: a new worker
starts from a spec, not a copy. Tests: the bootstrap gets one integration test instead of 12
`main_integration_test.go` files re-deriving it.

**Risk / migration.** The mains differ in real ways (traces on/off, per-chain flags, which
adapters they need); the spec must be additive and adopted one worker at a time.

**Size:** L (cross-area)
**Note:** Owned by the `cmd/`-wiring area; recorded here because it explains this area's
top-churn file.

## 4. Cross-area observations

- The "pack calls → `ExecutePinned` → `len(results) != len(calls)` check → per-index unpack"
  skeleton appears **24 times** repo-wide (`grep "results, got %d"`): 8 in `internal/pkg/blockchain`
  (this area), 10 in `morpho_indexer` (`blockchain_service.go` alone has 8), 3 in
  `morpho_indexer/vault_probe.go` + `adapter_probe.go`, 2 in `fluid_vault_indexer`. All of them
  could use `shared.RunSnapshotReads`. This is probably the single highest-leverage shared seam in
  the repo.
- `internal/services/shared/` already holds `RunSnapshotReads`, `UnpackUint`, `UnpackSingleUint`,
  `UnpackUintArray` and `OptionalUintResult` — built for the DEX indexers and unknown to the
  pricing and morpho spines. Worth an audit of what else in `shared/` has exactly one consumer.
- The hand-built multi-row `VALUES` placeholder loop (`baseIdx := i * 7`) appears in 4 postgres
  repositories: `offchain_price_repository.go:176`, `onchain_price_repository.go:234`,
  `protocol_repository.go`, `user_repository.go`. All four also repeat the same
  sort → `Begin` → chunk → `ON CONFLICT DO NOTHING` → `Commit` envelope; the on/offchain price
  pair differ only in the comparator, the table and the conflict target. One generic
  `batchUpsert[T]` helper would absorb all four.
- Three near-identical k8s deployments exist for one worker binary
  (`k8s/base/oracle-price-worker`, `base-oracle-price-worker`, `avalanche-oracle-price-worker`),
  which is why the Avalanche commit `32ee9563` needed 9 k8s files for a 1-line Go change. The
  per-chain overlay pattern looks like the real cost of adding a chain.
- `entity.ChainIDToName`, `entity.ChainIDToS3Bucket`, `allocation_tracker/chains.go` and the two
  vendor maps in `sky`/`skydata` are five overlapping chain tables with three different spellings
  for mainnet and two for Avalanche. A single chain registry with named spellings per context
  would settle it.
- `stl-verify/AGENTS.md`'s architecture section and its "New Use Case / New External Dependency"
  recipes describe the dead inbound hexagon (F04.6) and a `services/` layout that no longer
  matches the tree. Whoever owns the AGENTS.md chain should re-derive it from the code.
- `internal/pkg/skyenvelope` (56 lines: `RequireFullPage`, `OptionalText`, `OptionalNumber`) is a
  genuinely deep little module — two adapters, real invariant (a page outgrowing its limit fails
  loudly). Worth citing as the shape other shared helpers should aim for.

## 5. Open questions

- Are `GetOracle`, `GetLatestBlock`, `GetOracleByAddress`, `InsertOracle`,
  `InsertProtocolOracleBinding` and `CopyOracleAssets` (F04.5) leftovers, or are they called from
  the Python side / a runbook via SQL? `CopyOracleAssets` has 8 integration-test references and
  its migration comment suggests an operational purpose I could not confirm from Go alone.
- Does any live feed's price actually exceed `float64` precision at 18 decimals (F04.8)? That
  determines whether the fix is a correctness matter or hygiene. A query against
  `onchain_token_price` for Chronicle-sourced rows would answer it.
- Is `oracle_backfill`'s fail-soft-per-block behaviour (F04.3) a deliberate operational choice —
  e.g. because ranges routinely include pre-deployment blocks that the `validFrom` clamp does not
  fully cover — or has nobody looked at the exit code? The `erc4626_share_prices.go:80-86` comment
  records the behaviour but not a justification.
- Was the Aave live/backfill read asymmetry (batched `getAssetsPrices` vs individual
  `getAssetPrice`, F04.2) forced by historical blocks where the oracle reverts for unconfigured
  assets? If so, the unification needs a registry-driven per-asset `validFrom`, which is a bigger
  change than the extraction itself.
- `oracle_price_worker` validates feed decimals lazily, on the first block processed
  (`service.go:334-341`), not at startup. Deliberate (the check needs a block number) or an
  accident? It means a decimals mismatch stalls the SQS queue rather than failing the pod.
