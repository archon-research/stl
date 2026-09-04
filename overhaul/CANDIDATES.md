# Refactoring candidates

Status: DRAFT v1 — synthesised from findings 01, 03–11. Findings 02 (allocation/prime/aave),
12 (git-history metrics) and 13 (python/ts/k8s/alerts) are still to be merged in.

Each candidate is one program of work that several area reports point at from different sides.
`F<area>.<n>` ids reference `findings/`. Vocabulary (module, seam, depth, deletion test) is in
`BRIEF.md`. Sizes: S < 300 lines, M < 1000, L = 2–4 PRs, XL = an epic of PR-sized slices.

## Reading the whole thing in one paragraph

The repo is hexagonal in name but not in shape. 59 port interfaces exist and exactly two have a
second production adapter; 44 port methods take a `pgx.Tx`, so a second adapter is impossible.
Every one of the 8 SQS block workers, 26 Postgres repositories, 11 telemetry structs and ~35
multicall read sites is a hand-copied skeleton, and the copies have drifted into production bugs
(a worker still pinning reorg-sensitive reads by block number, a backfiller exiting 0 with every
block failed, a block stream that an SNS misconfiguration would silently delete). Block identity
has no type, so one invariant change rewrote 59 signatures in 74 files. Meanwhile the good deep
modules the repo needs already exist — `dexbootstrap`, `sqsutil.RunLoop`, `dexconsumer`,
`shared.RunSnapshotReads`, `temporal.RunCronjob`, `testutil.MockMulticaller`, `RunShared`,
`AppendOnChange`, `dextelemetry`, `wsclient`, `httpclient` — each adopted by one family and
ignored by the rest. The overhaul is mostly *finishing adoptions that were started*, plus two
new value types (`Block`, opaque `Tx`) that let the existing seams enforce what today is prose.

## Candidate index

| # | Candidate | Strength | Size | Depends on |
|---|---|---|---|---|
| C1 | Block identity as a value; block-pinned `StateReader` seam | Strong | XL | — |
| C2 | One block-event worker runtime (strict contract, shared runner, worker kit, one SQS loop) | Strong | XL | F10.7 first |
| C3 | One on-chain read dialect: `ReadNamed` everywhere, one ERC20 reader, parsed-once ABIs | Strong | L–XL | C1 |
| C4 | Registries instead of hand-maintained lists and type switches | Strong | L | — |
| C5 | Postgres persistence core; append-only enforced by the database | Strong | XL | — |
| C6 | Ports and domain: opaque `Tx`, ports grouped by capability, entities out of ports | Strong | XL | C5 |
| C7 | Delete dead and misplaced code | Strong | S–M | — |
| C8 | Worker and backfiller share one pipeline | Strong | L | C1 helps |
| C9 | One telemetry registration module, one chain label | Strong | M–L | — |
| C10 | Correctness fixes to land now (bug tickets) | Strong | S–M each | — |
| C11 | External-API client plumbing: one HTTP client, one config idiom, one error taxonomy | Strong | M–L | — |
| C12 | Test infrastructure: generated doubles, `testutil` split, size and boundary lint | Strong | M–L | — |
| C13 | Build and deploy: Makefile docker targets, image roster, one image | Worth exploring | M–L | C2 |
| C14 | Split god files and grab-bag packages into cohesive modules | Worth exploring | M–L each | C1, C2 |

---

## C1 — Block identity as a value; block-pinned `StateReader` seam

**Strength** Strong · **Size** XL (~8–10 PRs of M/L) · **Feeds from** F08.1, F09.3, F01.3,
F08.6, F03.2, F03.6, F05.13, F05.9, F09.9, F09.10, F03.7, F04.1 (its `BlockRef` half)

**Problem.** There is no type for "which block". An AST scan finds **125 non-test functions**
threading three or more of (chainID, number, hash, version, timestamp) as loose parameters, in two
incompatible spellings, with `chainID` as `int64` in 160 places and `int` in 19 (F09.3). The
reorg-correctness invariant — state reads pin to the block *hash* — is re-decided at **51**
multicall call sites (28 by hash, 23 by number) and documented in prose at ~14 of them (F08.1).
The half of the invariant the seam does enforce (never read `latest`) has zero violations; the
half left to convention has a live worker on `main`, `fluid-vault-indexer`, number-pinned end to
end (F03.2, `blockchain_service.go:266`). The archiver smuggles number and version through
`context.Value` because the read carries no identity, and fluid forgets one of the two stamps
(F08.6). Adding the hash in VEC-471 changed **59 signatures across 74 files** (F01.3). Three
packages hand-roll a "was it pinned?" test assertion three different ways; 11 bespoke
`Multicaller` doubles exist beside the shared one (F08.1, F03.7).

**Shape.** One value type in `domain/entity` (the only dependency-free layer) and a reader bound
to it:

```go
type Block struct { ChainID ChainID; Number int64; Version int; Hash common.Hash; Timestamp time.Time }
func BlockFromEvent(e BlockEvent) (Block, error)      // absorbs ParsedBlockHash's guard, once
func (b Block) CacheKey(dataType string) string        // absorbs redis/blockcache.go:286 and rawsckey

type StateReader interface {                           // a Multicaller already bound to one block
    Block() entity.Block
    Read(ctx, calls []Call) ([]Result, error)          // pin + count-check + archive stamp, once
    ReadNamed(ctx, reads []SnapshotRead) error         // today's shared.RunSnapshotReads
}
type StaticProber interface { ProbeAt(ctx, number int64, calls []Call) ([]Result, error) }
func PinTo(mc Multicaller, b entity.Block) StateReader
```

Handlers take a `StateReader`, not a tuple; an unpinned versioned read becomes unrepresentable.
`Multicaller` stays as transport with its three adapters.

**Deletion test.** Delete it and the pin decision reappears at 51 sites and the tuple at 125
signatures. Earns its keep.

**Migration.** (1) `entity.Block` + `PinTo`/`Prober`, no callers. (2) Convert
`shared.RunSnapshotReads` to `StateReader.ReadNamed` (3 callers already have the shape).
(3) One indexer per PR, innermost first; **fluid first**, because its conversion is the bug fix.
(4) `BlockCacheReader.GetBlock(ctx, ref)` — one conversion covers 23 of the 125 sites.
(5) Entity constructors take `Block`. (6) Unexport `Multicaller` from services.

**Metric moved.** Number-pinned state reads outside `StaticProber`: 23 → 0. Functions with ≥3
block params: 125 → ~0. Bespoke `Multicaller` doubles: 11 → 0.

---

## C2 — One block-event worker runtime

**Strength** Strong · **Size** XL (lands one worker per PR; rehearsed once already) ·
**Feeds from** F10.1, F10.7, F10.2, F10.3, F10.5, F10.6, F10.10, F01.14, F01.12, F05.1, F05.2,
F05.8, F05.16, F03.3, F03.4, F03.17, F04.10, F04.16, F06.9, F08.12, F11 cross-area (4 copies of
`TestParseConfig`), F06 cross-area (build metadata in 21 mains; 11 localhost DSN defaults)

**Problem.** This is the "why PRs are big" candidate. Across the 7 SQS worker roots, 1,480
substantive lines hold only 489 distinct ones (**67% duplicate**); `morpho` and `fluid-vault`
share 172 identical lines of 229 (F10.1). Of 70 edits to the five most-churned `main.go` files
since March, **47% were mechanical ripple** from a signature change elsewhere; one `defer cancel()`
fix touched 26 mains; commit `58f9c196` repaired four production bugs that existed only because
the copies drifted, then deferred consolidation (F10.1). Layered on top: 8 hand-rolled
`BlockEventHandler`s beside the one shared runner `dexconsumer.BlockProcessor` (F01.14); two
complete SQS consume-loop designs, the newer and safer `sqsutil.RunLoop` having **one** caller
(F10 cross-area, F05.2); the reorg re-snapshot rule carried by `dexconsumer.DueSet` and skipped by
fluid and psm3 (F03.3); three ack/retry policies for a failed block (F03.17); and a block-event
contract that is `json.Unmarshal` into a bare struct, so an SNS envelope without raw delivery
decodes to an all-zero event that the loop deletes as "foreign chain" (F05.1). Six workers
default `CHAIN_ID` to mainnet when the variable is missing (F10.3). 11 of 32 binaries never
initialise telemetry (F10.5).

**Shape.** Three modules the repo already has in embryo:

1. `blockevent` — `Decode(body) (Event, error)` strict at the boundary; `Event.Block()` returns
   the C1 value; a `blockpayload.Reader` exposes `Receipts(ctx, block)` so the six copies of the
   read-and-nil-check skeleton become one call (F05.1).
2. `internal/workerkit` — `dexbootstrap` promoted out of `cmd/workers/internal/` (F10.7) with the
   shape both existing kits share: `ParseConfig` → `Deps` → `Worker.Build(ctx, *Deps)
   (lifecycle.Service, error)` → `Run`. Every SQS worker root becomes ~10 lines, like the cronjobs
   already are. `Build` lives next to the service it constructs, so a signature change and its
   call site land in one file.
3. One runner: `dexconsumer.BlockProcessor` + `sqsutil.RunLoop` become the only way a block
   reaches a handler; the handler signature is `Handle(ctx, StateReader, Event) error` from day one
   so C1 and C2 meet at one seam.

**Deletion test.** The kit exists twice already (`temporal.RunCronjob` for 7 cronjobs,
`dexbootstrap` for 3 DEX binaries) and each collapse deleted hundreds of lines. Earns its keep.

**Migration.** F10.7 move (S, no behaviour change) → strict `blockevent.Decode` (S) → adopt one
worker per PR, cheapest first: morpho + fluid, then sparklend, prime-allocation, oracle-price,
psm3, prime-debt, raw-data-backup. Each adoption *changes behaviour where copies drifted*
(`CHAIN_ID` required, S3 bucket validation, range checks) and must say so. Then retire
`shared.SQSConsumerConfig` and the 7 bespoke `SQSConsumer` doubles.

**Metric moved.** `main.go` commits that are ripple: 47% → ~0. Lines per worker root: ~170 →
~10. New-indexer fixed cost in `cmd/`: one file.

---

## C3 — One on-chain read dialect

**Strength** Strong · **Size** L–XL · **Depends on** C1 · **Feeds from** F08.2, F01.2, F04.7,
F08.3, F03.13, F09.9, F08.4, F03.8, F03.10, F03.5, F03.1, F03.9, F08.11, 03 cross-area (three
in-service multicall adapters with no port)

**Problem.** The pack → execute → count-check → unpack skeleton is written **~35 times** across
17 files with divergent count checks (`<` vs `!=`) (F08.2); Morpho alone has 18 copies (F01.2),
pricing 8 (F04.7). `shared.RunSnapshotReads` and `UnpackUint` exist for exactly this and are used
only by Curve and Uniswap. **Four** ERC20-metadata readers, three `TokenMetadata` types, three
caches, three failure policies (F08.3). **30** ABI getters return `(*abi.ABI, error)` and re-parse
JSON on every call, including per-pool-per-block in `uniswapv3indexer.SnapshotState` (F08.4).
Curve's two handlers are one handler with two ABIs: **617 of 1,159 lines line-identical**, caused
by 40 hand-written 22-line `SnapshotRead` builders where `uniswapv3indexer/state.go:171-246` does
the same job with three combinators (F03.1). `morpho_indexer/blockchain_service.go` (1,408),
`pkg/aavelike/blockchain_service.go` (1,124) and fluid's `blockchain_service.go` are all multicall
adapters living outside `adapters/`, with no port (03 cross-area, F03.5).

**Shape.** `StateReader.ReadNamed` (from C1) is the one dialect; an `abis` registry parsed once at
init returning `*abi.ABI` (no error); one `erc20meta.Reader` behind one `TokenMetadata` type; a
`SnapshotRead` combinator library (`Uint`, `OptionalUint`, `Address`, `Tuple`) so a pool/vault/
market snapshot is a table of reads, not 22 lines per field. The three in-service chain adapters
move behind ports.

**Deletion test.** Delete `RunSnapshotReads` and 35 skeletons reappear. Earns its keep; it just
needs to be adopted.

**Metric moved.** Copies of the skeleton: ~35 → 0. `TokenMetadata` types: 3 → 1. Curve
handler lines: 2,182 → ~1,200.

---

## C4 — Registries instead of hand-maintained lists and type switches

**Strength** Strong · **Size** L · **Feeds from** F01.1, F01.5, F01.6, F04.1, F03.10, F04.11,
F09.9, 04 cross-area (five overlapping chain tables)

**Problem.** One Morpho VaultV2 event name appears at **18 non-test sites in 6 hand-maintained
lists** that must agree; `replay.go:190` says "Keep in sync with the dispatch switch" (F01.1).
`OracleType` is dispatched at **9 sites in 3 packages plus 2 mains**; the two commits that added an
oracle type touched 21 and 25 files, while adding a *feed* is a one-file migration (F04.1). Chain
identity lives in five overlapping tables (`entity.ChainIDToName`, `ChainIDToS3Bucket`,
`allocation_tracker/chains.go`, `sky`, `skydata`) with three spellings for mainnet (04 cross-area,
F04.11). Curve's two 200-line `liquidity_decode.go` functions are an ABI arg-spec table written as
a switch (F03.10).

**Shape.** Per protocol, one registry table (event name → ABI, decoder, handler, replay policy,
telemetry label) that every consumer iterates; an `OraclePricer` interface with a registry keyed
by `OracleType`; a single `chain` registry with named spellings per context.

**Deletion test.** Delete a registry and N lists reappear. Earns its keep.

**Metric moved.** Files touched to add one Morpho event: 6 → 1. To add one oracle type: 21–25 →
~3.

---

## C5 — Postgres persistence core; append-only enforced by the database

**Strength** Strong · **Size** XL (each helper family is one M PR) · **Feeds from** F07.1, F07.2,
F07.3, F07.4, F07.5, F07.9, F07.10, F07.12, F01.11, F03.12, F05.5, 04 cross-area (VALUES loop in
4 repos), 03 cross-area (no-op `DO UPDATE` in fluid/maple/morpho repos)

**Problem.** 26 repositories, 28 constructors, **no two alike**; 48 hand-written `rows.Next`
loops and 0 uses of pgx generics; 17 `SendBatch` sites with 5 close-error conventions; 3
bulk-write strategies and no `COPY` (F07.3, F07.5). Registry get-or-create exists in **9 SQL
idioms**; the decimals-drift guard is wired into 1 of 4 paths; `resolveTokenIDs` is copy-pasted
and both copies N+1 past an existing batch API (F07.4). `AppendOnChange` has 2 callers while
uniswap and morpho rebuilt its batched form by hand (F07.2). Append-only is the documented
default but migration `20260122_140100:71` grants `UPDATE, DELETE` on every new table by
`ALTER DEFAULT PRIVILEGES`; enforcement is opt-out across three disjoint lists, and
`block_states.is_orphaned`/`block_published` are rewritten in place at 4 sites on the
reorg-critical hypertable (F07.1). `position_repository.go:107` builds a combined close error;
`:176` drops it silently — adjacent functions (F07.3). `query_telemetry.go` is the shape the rest
should copy (F07.12).

**Shape.** One `internal/adapters/outbound/postgres/pgcore` (or in-package) layer: `WithTx`,
`ScanAll[T]`, `BatchExec` with one close convention, `BatchUpsert[T]`, `AppendOnChange[T]`
(advisory lock + read-latest + append), `Registry.Resolve*` batch resolvers with the drift guard
always on. Database default flipped: REVOKE by default, a single allowlist of tables that may
`UPDATE`, asserted by one test against `schema_master.json`. `block_states` lifecycle flags become
versioned rows.

**Deletion test.** Delete the core and 48 scan loops, 9 idioms and 3 append-on-change copies
reappear. Earns its keep.

**Metric moved.** Hand-written scan loops: 48 → 0. Registry idioms: 9 → 1. Tables writable in
place: 128 → the allowlist.

---

## C6 — Ports and domain: opaque `Tx`, ports by capability, entities out of ports

**Strength** Strong · **Size** XL (one bounded context per PR) · **Depends on** C5 ·
**Feeds from** F09.1, F09.2, F09.5, F09.6, F09.8, F09.11, F09.12, F09.13, F07.6, F05.14, F08.7,
F04.5, F05.5

**Problem.** **18 of 45** port files import `jackc/pgx/v5`; **44 methods take `tx pgx.Tx`**. The
port names its own driver, so a second adapter is impossible, which is why a whole-module
`types.Implements` scan finds **2 real seams out of 61** interfaces (F09.1, F09 metrics). 22 ports
have the same basename as 22 postgres files; the 28 adapter structs behind them are one struct
wearing 28 names, wired by hand at **86** `postgres.New*` call sites (F09.2). `ports/outbound`
holds **44 data structs** including `BlockEvent`/`BlockState`/`ReorgEvent`, while 52 of 87 domain
structs are named exactly like a DB table with 105 surrogate-key fields and 86 `ID <= 0` checks:
the layers have swapped jobs (F09.5). 45 ports have ≤3 methods, 14 interfaces cover 6 structs, and
one 24-method god port covers six concerns (F09.8, F05.5). `BlockchainClient` has 7 of 13
methods with no production caller; `OnchainPriceRepository` 6 of 12 (F08.7, F04.5).

**Shape.** F09.12's target: ~16 ports in ~10 files — `block.go`, `blockstate.go` (three ledgers),
one `*Store` per bounded context with a block-scoped `SaveBlock(ctx, tx, writes)` (Curve and
Uniswap already have it), `feeds.go`, `chainio.go`, `infra.go`. `outbound.Tx` is opaque;
`TxManager` is the only thing that knows pgx. Block-domain types move to `entity`; the port
declared inside `domain/entity` moves out (F09.13). `internal/common` merges into `internal/pkg`
(F09.11).

**Deletion test.** Today most ports *fail* it: delete a one-adapter, one-caller port and nothing
reappears but a mock. After grouping, each `Store` is a real seam with a real fake.

**Metric moved.** Ports: 59 → ~16. Ports importing pgx: 18 → 0. `postgres.New*` call sites:
86 → ~15.

---

## C7 — Delete dead and misplaced code

**Strength** Strong · **Size** S–M total, one or two PRs · **Feeds from** F04.6, F09.4, F05.7,
F08.9, F07.7, F03.14, F10.11, F03.16, F06.1 (utils.go), F11.7, F08.10, F06.13, F01.15, F11.13,
F08.7, F04.5, F05.14, F04 cross-area (AGENTS.md recipes describe the dead code)

**Inventory.**
- The entire inbound hexagon: `adapters/inbound/http` (~450 lines), `ports/inbound`,
  `services/verification_service.go`. No binary imports it; `HealthChecker`'s doc names an
  implementation that does not exist; the k8s probe is `pgrep -f watcher`. Also delete the
  "New Use Case" recipe in `stl-verify/AGENTS.md` that points new work at it.
- `adapters/outbound/memory` (1,599 lines): zero non-test importers; test doubles in the
  production adapter tree, inflating every "two adapters" claim. Move what tests need next to
  the shared doubles, delete the `Repository` template.
- `testutil/mockchain` (1,776 src lines): a deployed service in the test-helper tree, imported by
  no test. Move to `cmd/util/mock-blockchain-server`'s own package.
- `cmd/util/generate-er` (807 lines): output covers 32 of 128 tables, last regenerated 89
  migrations ago; `schema_master.json` already exists.
- Two `cmd/` directories holding only an untracked `.env`; `shared.LogBelongsTo` and
  `shared.FormatAmount`; `pkg/testutils` (17 lines, one letter from `testutil`);
  `blockverifier` factory with one kind; `proxytls` HTTPS support nothing wires; the
  `morpho.receipt.duration_seconds` metric declared and never recorded; dead methods on
  `BlockchainClient`, `OnchainPriceRepository`, `BlockCache`, `BlockStateRepository`.

**Metric moved.** ~5,000 lines and two layers gone; `SYSTEM-MAP.md` binary count 34 → 32.

---

## C8 — Worker and backfiller share one pipeline

**Strength** Strong · **Size** L · **Feeds from** F04.2, F04.3, F04.9, F01.10, F05.3, F05.12,
08 cross-area (`blocktime` memo unused by `oracle_backfill`); report 02 will add the Aave/
Sparklend pair

**Problem.** `oracle_price_worker/service.go` (759) and `oracle_backfill/service.go` (702) hold
**10 matched function pairs** and five undocumented divergences: Aave reads batched
all-or-nothing live vs per-asset silently skipped in backfill, so the same block yields different
data; hash- vs number-pinning; cache updated before vs after the write; telemetry on one side;
opposite error policies (F04.2). `cacheAndPublishBlockData` exists twice with opposite
`MarkPublishComplete` failure policy (F05.3). Three independent notions of "the version at height
N", one derived from S3 by a hand-rolled JSON scanner that never reads `block_states` (F05.12).
Two channel pools and two bisect-retry loops across the Morpho backfiller and bootstrap (F01.10).

**Shape.** One `Pricer`/`Indexer` core per protocol taking a `StateReader` (C1); the worker and the
backfiller are two *drivers* (live events vs a block range) over the same core. Divergences
become explicit options or disappear.

**Metric moved.** Matched function pairs: 10 → 0. Behavioural divergences between live and
backfill: 5 → 0 (documented).

---

## C9 — One telemetry registration module, one chain label

**Strength** Strong · **Size** M–L (one PR per package) · **Feeds from** F06.5, F01.8, F01.15,
F06.11, F04.15, F10.5, F03.15, F09 cross-area (recorder ports in `outbound`), 10 cross-area
(`ChainName` from 8 roots)

**Problem.** **11 per-package `Telemetry` structs** (~2.2k lines) repeat one registration skeleton
with four constructor signatures; `pkg/dextelemetry`'s own doc says it deduplicated this for 3
workers "instead of duplicating it" and the other 8 never got it (F06.5). The `chain` label every
alert groups by is produced three ways, one a hardcoded `"ethereum"`. 11 of 32 binaries never
initialise telemetry (F10.5); the off-chain price path has no telemetry while the on-chain path
has 7 metrics and 4 alerts (F04.15). Fluid reports liveness through a port named for the backup
worker (F03.15).

**Shape.** `dextelemetry` generalised to `pkg/indexertelemetry` (instrument set + labels resolved
once from `Config`); `chainName` resolved once at the config boundary in the worker kit (C2).

**Metric moved.** `Telemetry` structs: 11 → 1. Binaries without telemetry: 11 → 0.

---

## C10 — Correctness fixes to land now

**Strength** Strong · **Size** S–M each · Independent of the refactors; each wants a ticket.

| Finding | Bug | Size |
|---|---|---|
| F03.2 / F08.1 | `fluid_vault_indexer` pins per-block vault snapshots by block number; reorg can return another fork's state | M |
| F03.3 | fluid and psm3 skip the `DueSet` reorg re-snapshot rule; orphaned fork's row stays latest | L |
| F05.1 | SNS envelope (no raw delivery) decodes to an all-zero event that is deleted as foreign-chain; no DLQ | S for the strict decoder |
| F04.3 | `oracle_backfill` counts failed blocks, returns nil, `main` prints "completed successfully", exit 0 | M |
| F10.3 | Six workers default `CHAIN_ID` to `"1"`; a missing var silently indexes mainnet | M |
| F07.1 | `block_states.is_orphaned` / `block_published` updated in place on the reorg-critical hypertable | M |
| F01.4 | Two transactions per log; the audit row commits separately from the structured row | L |
| F06.10 | CoinGecko warn-and-drops malformed rows into a silently short series feeding the price backfiller | S |
| F06.2 | `httpclient.isNonRetryable` misses `Unwrap() []error`; a non-retryable inside `errors.Join` is retried | S |
| F05.13 | Backfill compares canonical vs stored hash case-sensitively under a comment saying it must not | S |
| F08.5 | Watcher block-payload path retries permanent JSON-RPC errors in `call()` but not `callBatch()`; no jitter | M |
| F08.13 | `VatCaller.ReadDebts` reports per-vault failures in-band with a nil error | S |
| 03 cross-area | `pkg/aavelike/blockchain_service.go:610-612` ignores three `Pack` errors | S |
| F04.4 | "non-positive is not a price" enforced at 8 sites with 3 verdicts; the entity permits zero | M |
| F04.8 | Oracle prices round-trip through `float64` into `NUMERIC(30,18)`; change detection compares floats | M |
| F06.6 | Retry-stable snapshot timestamp honoured by 1 of 6 cronjobs | S–M |
| 08 cross-area | `alchemy/subscriber.go:406` drops a block header when the channel is full; needs an explicit decision | decision |
| 05 cross-area | Cache-miss fallback is Redis→S3 for indexers and Redis→RPC for backup; a payload absent from both is a permanent FIFO stall | decision |

---

## C11 — External-API client plumbing

**Strength** Strong · **Size** M–L · **Feeds from** F06.2, F06.3, F06.4, F06.7, F06.10, F04.12,
F04.13, F08.8, F08.5, F06.13

**Problem.** `pkg/httpclient.DoRequest` hardcodes GET, so `maple/client.go:1066-1163` keeps a
drifted copy (F06.2); six clients spell "apply config defaults" six ways (F06.3); five sibling
adapters use four error taxonomies (F06.10); `anchorage_tracker` keeps a whole HTTP adapter inside
a service with no retry or telemetry (F06.4); the shared client that carries every off-chain feed
has 27 test lines (F04.13). Four retry frameworks exist; the watcher's block-payload path bypasses
the one that is repo policy (F08.5). `pkg/wsclient` owns "dialing, deadlines, pongs, keepalive,
clean shutdown" for one consumer while `alchemy/subscriber.go`, the system's most critical
WebSocket, hand-rolls all of it (F08.8).

**Shape.** `httpclient` gains methods and a body; one `Config` with one defaulting idiom; one
`apierr` taxonomy (`Retryable`, `NotFound`, `Malformed`); `retry` is the only retry; the alchemy
subscriber sits on `wsclient`.

---

## C12 — Test infrastructure

**Strength** Strong · **Size** M–L · **Feeds from** F11.1, F09.7, F03.7, F05.16, F11.6, F11.8,
F11.3, F10.4, F11.4, F11.5, F11.9, F11.11, F11.12, F01.13, F07.8

**Problem.** **148 hand-rolled port doubles** in 76 files; 11 ports have ≥3 independent copies
(`SQSConsumer` 8, `TxManager` — a one-method port — 10); `testutil/mock_*.go` already uses the
exact `XxxFn func(...)` shape `moq` generates, typed by hand (F11.1). Three fake Ethereum JSON-RPC
servers and six copies of the `aggregate3` result struct (F11.6). No `funlen`/`gocognit` although
AGENTS.md calls it planned, so a **1,854-line test function** and six production functions over
200 lines pass CI (F11.3); no architecture-boundary lint. 114 integration-tagged files sit outside
the default lint gate (F11.4). CI shards are two hand-written manifests that must list all 123
packages (F11.5). Good news that constrains the design: `RunShared`/`SetupTestDB` are genuinely
deep (41 of 46 `TestMain`s are one statement) and should be the template, not a target (F11
correction, F07.11).

**Shape.** `moq`-generated doubles in one `testutil/fakes` package, hand-rolled ones deleted as
each port is touched by C6; `testutil` split into `fakes`, `containers`, `db`; `funlen`,
`gocognit` and `depguard` (services may not import adapters; ports may not import pgx) enabled
with `--new-from-rev` so the backlog is a ratchet, not a wall; shard manifests generated.

**Metric moved.** Hand-rolled doubles: 148 → ~0. Functions over the limit: ratcheted to 0.

---

## C13 — Build and deploy

**Strength** Worth exploring · **Size** M–L · **Depends on** C2 · **Feeds from** F11.2, F10.8,
F11.10, 04 cross-area (three near-identical k8s deployments per worker per chain); report 13
will add k8s detail

**Problem.** 951 of 2,846 Makefile lines are 82 near-identical docker targets although a
parameterised helper already exists and one of three families uses it (F11.2). One image per
binary costs 21 target groups, 24 roster lines and 54 Deployments; the stated blocker does not
block (F10.8). Container image tags are declared twice and kept in sync by a 100-line grep script
(F11.10). The Avalanche oracle commit needed 9 k8s files for a 1-line Go change.

**Shape.** Once C2 makes every worker a `Worker` value, one `stl-worker <name>` image with a
registry, one Deployment template per chain.

---

## C14 — Split god files and grab-bag packages

**Strength** Worth exploring (mostly falls out of C1–C6) · **Size** M–L each · **Feeds from**
F05.6, F04.14, F05.5, F06.1, F08.11, F01.9, F06.8, F10.9, F05.10, F11.8, F11.12

**Inventory.** `backfill_gaps_service.go` (1,371 lines, five concerns, top-level loops
log-and-continue); `live_data_service.go` (1,163, the watcher's reorg logic misfiled under a
price-sounding name); `BlockStateRepository` (25 methods, six concerns → three ledgers);
`services/shared` (four unrelated modules: DEX decode helpers, watcher telemetry, config, dead
utils); `pkg/blockchain` (four concerns under a name that promises chain access);
`processReceipt` (137 lines); Maple's four sync phases; `raw-block-bulk-downloader/main.go` (893)
and `generate-er/main.go` (807) as whole programs in `main.go`; seven test files over 2,500 lines.
