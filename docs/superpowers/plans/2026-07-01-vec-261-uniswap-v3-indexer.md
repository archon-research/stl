# VEC-261 Uniswap V3 Indexer — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A new Uniswap V3 indexer worker that captures the complete on-chain data surface (all 9 pool events, full pool-level state, authoritative per-tick liquidity map) for 18 active wstETH/stETH pools, plus a maintainability-driven hoist of cross-DEX plumbing out of `curveindexer` into the shared layer that also fixes two Curve blocking bugs.

**Architecture:** Hexagonal, BlockHandler model, mirroring `curveindexer`. New worker consumes the watcher block stream over SQS via the VEC-328 shared base (dexbootstrap/dexconsumer/dextelemetry). Per touched block: decode logs → multicall pool-level + touched-tick state pinned to the block hash → persist all facts/state/events in one transaction. Design: `docs/superpowers/specs/2026-07-01-vec-261-uniswap-v3-indexer-design.md`.

**Tech Stack:** Go 1.26, pgx v5 batch, TimescaleDB hypertables, go-ethereum ABI + Multicall3, OTel.

## Global Constraints

- Go 1.26+. Dependencies flow inward (domain → ports → adapters); never import adapters in domain/services.
- Append-only everywhere. Every new hypertable gets `processing_version` (INT, correction version) + `build_id` (INT, audit) and an advisory-locked `assign_processing_version_*` BEFORE INSERT trigger; all inserts `ON CONFLICT DO NOTHING`. No UPDATEs.
- Advisory locks acquired on the natural key in **sorted order** (deadlock-free). Latest-row selection orders by snapshot-time key `DESC` then `processing_version DESC` — **never** `build_id`.
- NEVER modify an applied migration file in `stl-verify/db/migrations/`. Always add new files (timestamped).
- Every new table AND column gets a `COMMENT ON` in a sibling comments migration, matching the `[Type]`/`Roles`/unit-scale style of `20260609_120000_add_schema_comments.sql`. Numeric columns MUST state unit/scale.
- Core pool-level multicalls use `AllowFailure:false` and map to NOT NULL columns (every V3 pool always has them). Only `observe()`/TWAP uses `AllowFailure:true` + nullable column. A reverted required read must error (never store NULL/0 silently). Never swallow errors — partial failure returns an error and leaves the SQS message for redelivery.
- RPC (multicall) runs BEFORE the pgx transaction opens (never pin a DB connection during RPC).
- State/tick multicalls are pinned to the block **hash** of the `(block_number, block_version)` being processed (reorg-correctness).
- The worker must NOT assume wstETH is token0 — read `token0()`/`token1()` at load (token1 in mstETH/urLRT/boxETH pools).
- Interfaces: behavior `-er`; ports keep noun patterns (`XxxRepository`, `XxxClient`). Constructors `New`. Files snake_case. `main()` at top of main.go; `run(ctx, args)` extracted for testability.
- Tests: table-driven, one behavior per function, mock only outbound ports (unit) / mock only the RPC data source (integration). Services + main.go target 100% coverage; test services via public API only; main.go gets integration tests only. Test helpers live in `_test.go` files only.
- Functions compose like prose: the BlockHandler reads as an outline of named helpers.
- Comments explain *why*, not *what*; default to none.

**Reference template (mirror its structure, adapt specifics):** the Curve worker at `stl-verify/cmd/workers/curve-indexer/`, `stl-verify/internal/services/curveindexer/`, `stl-verify/internal/services/dexconsumer/`, `stl-verify/internal/adapters/outbound/postgres/curve_repository.go`, migrations `20260521_11000_*`/`20260521_120000_*`.

---

## Phase A — Shared hoist + Curve fixes (foundation)

> These refactor existing, tested Curve code. **Curve's unit + integration tests must stay green after every task.** Run `cd stl-verify && make test` (and the curve integration tests) at each task's verify step. Keep each hoist a pure move + delegate (no behavior change) except the two explicit bug fixes.

### Task A1: Fix A2 — pin state multicall to block hash

**Files:**
- Modify: `stl-verify/internal/ports/outbound/` block-event type (add `BlockHash common.Hash`) — locate via `grep -rn "type BlockEvent" stl-verify/internal`
- Modify: `stl-verify/internal/services/curveindexer/service.go` (`snapshotPools`), `stableswap_handler.go` + `cryptoswap_handler.go` (`SnapshotState` multicall call sites)
- Modify: `stl-verify/internal/ports/outbound/multicaller.go` if needed to accept a block hash
- Test: `stl-verify/internal/services/curveindexer/service_test.go`

**Interfaces:**
- Consumes: existing `Multicaller.Execute(ctx, calls, blockNumber *big.Int)`.
- Produces: multicall pinned by block hash. Prefer adding `ExecuteAtHash(ctx, calls, blockHash common.Hash) ([]Result, error)` to `Multicaller` (Multicall3 via `eth_call` with a block-hash block tag), keeping `Execute` for callers that only have a number. `BlockEvent.BlockHash` populated from the watcher payload.

- [ ] **Step 1:** Inspect `BlockEvent` and the multicaller adapter (`grep -rn "func.*Execute" stl-verify/internal/adapters/outbound/.../multicall`); confirm the watcher's SNS/SQS payload carries the block hash (`grep -rn "BlockHash\|Hash" ` on the block-event producer). If the hash is absent from the payload, note it and pin by number as today but add a failing test documenting the gap (do not silently ship A2 half-done).
- [ ] **Step 2:** Write a failing test in `service_test.go` asserting `snapshotPools` calls the multicaller with the block **hash** (mock multicaller records the block tag).
- [ ] **Step 3:** Run: `cd stl-verify && go test ./internal/services/curveindexer/ -run TestSnapshotPinsBlockHash -v` — expect FAIL.
- [ ] **Step 4:** Add `ExecuteAtHash` to the `Multicaller` port + adapter (eth_call with `{"blockHash": ...}` object block tag), thread `BlockEvent.BlockHash` into `snapshotPools` → handlers.
- [ ] **Step 5:** Run the test + full curve suite: `go test ./internal/services/curveindexer/... ./internal/services/dexconsumer/... -v` — expect PASS.
- [ ] **Step 6:** Commit: `git commit -m "fix(dex): pin state multicall to block hash for reorg-correct reads (A2)"`

### Task A2: Fix A1 + hoist snapshot-set bookkeeping to dexconsumer

**Files:**
- Create: `stl-verify/internal/services/dexconsumer/snapshotset.go` + `snapshotset_test.go`
- Modify: `stl-verify/internal/services/curveindexer/service.go` (delete `buildSnapshotSet`/`snapshotKey`/`lastSnapshot` logic, delegate to dexconsumer)

**Interfaces:**
- Produces: `dexconsumer.SnapshotTracker` with `func NewSnapshotTracker(heartbeatBlocks int64) *SnapshotTracker` and `func (t *SnapshotTracker) DueSet(pools []SnapshotPool, touchedIDs []int64, bn int64, ver int) []int64` where `type SnapshotPool interface { PoolID() int64; DeployBlockNum() int64 }`. **A1 fix:** `DueSet` excludes any pool with `bn < DeployBlockNum()`; a *touched* pool below its deploy block returns an error path (registry bug) — expose `DueSetChecked(...) ([]int64, error)`.
- Consumes: `RegisteredPool` (curve) / `uniswapv3indexer` pool implement `SnapshotPool`.

- [ ] **Step 1:** Write failing tests in `snapshotset_test.go`: (a) heartbeat-due when unseen / `bn-last>=hb` / `bn==last && ver!=last`; (b) **excludes a pool with `bn < deployBlock`**; (c) touched pool below deploy block → error; (d) de-dupes touched+heartbeat; (e) returns sorted ascending IDs.
- [ ] **Step 2:** Run: `cd stl-verify && go test ./internal/services/dexconsumer/ -run TestSnapshotTracker -v` — expect FAIL (undefined).
- [ ] **Step 3:** Implement `snapshotset.go` (port the curve logic verbatim + the deploy-block gate).
- [ ] **Step 4:** Refactor `curveindexer/service.go` to construct and use `dexconsumer.SnapshotTracker`; delete the inlined logic.
- [ ] **Step 5:** Run: `go test ./internal/services/dexconsumer/... ./internal/services/curveindexer/... -v` — expect PASS.
- [ ] **Step 6:** Commit: `git commit -m "refactor(dex): hoist snapshot-set bookkeeping to dexconsumer + gate by deploy_block (A1)"`

### Task A3: Hoist ABI-log decode + unpack utils to shared

**Files:**
- Create: `stl-verify/internal/services/shared/abilog.go` + `abilog_test.go` (or extend existing `shared`)
- Modify: `stl-verify/internal/services/curveindexer/decode_helpers.go`, `event_decode.go` (delete moved funcs, import from shared)

**Interfaces:**
- Produces (in `shared`): `LogBelongsTo(log, addrs ...common.Address) bool`; `DecodeLog(ev abi.Event, log types.Log) (map[string]any, error)`; `GetAddrField/GetBigIntField/GetBigIntSliceField(m map[string]any, key string) (..., error)`; `ParseHexUint(s string) (uint, error)`; `AppendDecodedCaptured/AppendRawCaptured(...)`; `UnpackSingleUint(result []byte) (*big.Int, error)`; `UnpackUintArray(result []byte, n int) ([]*big.Int, error)`; `OptionalUintResult(res outbound.Result) (*big.Int, error)` (revert → error, never nil-swallow).
- Consumes: existing `shared.UnpackUint`.

- [ ] **Step 1:** Write `abilog_test.go` porting the existing curve tests for these helpers (they already exist in curve test files — move the assertions).
- [ ] **Step 2:** Run: `cd stl-verify && go test ./internal/services/shared/ -v` — expect FAIL (undefined).
- [ ] **Step 3:** Move the functions into `shared/abilog.go` (verbatim), generalizing `logBelongsToPool` to variadic addresses.
- [ ] **Step 4:** Update curve callers to import from `shared`; delete the local copies.
- [ ] **Step 5:** Run: `go test ./internal/services/shared/... ./internal/services/curveindexer/... -v` — expect PASS.
- [ ] **Step 6:** Commit: `git commit -m "refactor(dex): hoist ABI-log decode + unpack utils to shared"`

### Task A4: Hoist append-on-change algorithm to a postgres helper

**Files:**
- Create: `stl-verify/internal/adapters/outbound/postgres/append_on_change.go` + integration test
- Modify: `stl-verify/internal/adapters/outbound/postgres/curve_repository.go` (config writers delegate to the helper)

**Interfaces:**
- Produces: `func AppendOnChange(ctx, tx pgx.Tx, lockKey int64, readLatest func(pgx.Tx) (found bool, equal func(candidate any) bool, err error), insert func(pgx.Tx) error) error` — acquires `pg_advisory_xact_lock(lockKey)`, reads latest, inserts only if absent or changed. Field lists stay caller-specific. (Refine signature to the cleanest generic that fits both curve config writers; keep the deadlock-safe lock ordering.)

- [ ] **Step 1:** Write an integration test exercising `AppendOnChange`: insert-on-first, skip-when-equal, insert-on-change, idempotent replay.
- [ ] **Step 2:** Run: `cd stl-verify && make test-integration` (or the targeted package) — expect FAIL.
- [ ] **Step 3:** Implement `append_on_change.go`; refactor both curve config writers to use it.
- [ ] **Step 4:** Run the curve repository integration tests — expect PASS (append-on-change behavior unchanged).
- [ ] **Step 5:** Commit: `git commit -m "refactor(postgres): extract generic advisory-locked append-on-change helper"`

### Task A5: (Conditional) Generalize BlockHandler orchestrator

**Files:** Create `stl-verify/internal/services/dexconsumer/orchestrator.go` (+ test); refactor `curveindexer/service.go`.

**Interfaces (attempt):** generic `Orchestrator[Acc, Snap]` parameterized by `Decoder` (receipts → Acc), `Snapshotter` (pools+block → []Snap), `Writer` (Acc+[]Snap+tx → rows). If the concrete curve accumulator/writes/snapshot types cannot be expressed without leaking curve specifics into the shared type, **abandon this task, keep the orchestration duplicated per DEX, and document why** (maintainability > DRY, per the reviewer's flag). This is the only optional task.

- [ ] **Step 1:** Sketch the generic interfaces against BOTH curve's and (planned) uniswap's accumulator/writes shapes on paper (in the task notes). Decide GO / NO-GO.
- [ ] **Step 2 (GO):** Write orchestrator test; implement; refactor curve to use it; run curve suite green; commit `refactor(dex): generic block orchestrator`.
- [ ] **Step 2 (NO-GO):** Add a one-paragraph note in the design doc's §9 recording the decision; skip. No commit.

---

## Phase B — Uniswap V3 worker

### Task B1: Migration — Uniswap V3 tables

**Files:**
- Create: `stl-verify/db/migrations/20260701_100000_create_uniswap_v3_tables.sql`
- Test: `stl-verify/internal/adapters/outbound/postgres/uniswap_v3_migration_integration_test.go`

**Interfaces:**
- Produces tables (schemas per design §7): `uniswap_v3_pool` (registry), `uniswap_v3_pool_state` (hypertable), `uniswap_v3_swap` (hypertable), `uniswap_v3_liquidity_event` (hypertable), `uniswap_v3_tick` (append-on-change), `uniswap_v3_pool_event` (hypertable). Each hypertable: `create_hypertable` on `block_timestamp`, compression `segmentby` the pool id, `assign_processing_version_uniswap_v3_*` BEFORE INSERT trigger (advisory-locked, sorted key), PK includes `processing_version` (and `log_index` for fact tables). FKs: `chain_id`→chain, `protocol_id`→protocol, `token0_id`/`token1_id`→token.

- [ ] **Step 1:** Write the integration test: migrate up on a fresh test DB; assert each table exists, each hypertable is a hypertable (`_timescaledb_catalog.hypertable`), each `assign_processing_version_uniswap_v3_*` trigger exists, and the FKs/uniques exist. (Mirror `curve_migration_integration_test.go`.)
- [ ] **Step 2:** Run: `cd stl-verify && go test ./internal/adapters/outbound/postgres/ -run TestUniswapV3Migration -v` — expect FAIL.
- [ ] **Step 3:** Write the migration SQL. Mirror `20260521_110000_create_curve_dex_tables.sql` for the trigger/compression boilerplate; columns per design §7 (full slot0 fields, liquidity, feeGrowthGlobal0/1, protocolFees0/1, balance0/1, twap_tick+twap_window_secs nullable; swap signed amounts; liquidity_event name-CHECK + owner/ticks/amounts; tick liquidityGross/Net/feeGrowthOutside0/1/initialized; pool_event name-CHECK + params jsonb). Core columns NOT NULL.
- [ ] **Step 4:** Run the test — expect PASS.
- [ ] **Step 5:** Commit: `git commit -m "feat(uniswap-v3): create pool/state/swap/liquidity/tick/event tables"`

### Task B2: Migration — column comments

**Files:** Create `stl-verify/db/migrations/20260701_100100_uniswap_v3_comments.sql`; test in the same integration test file.

- [ ] **Step 1:** Write a test asserting zero uncommented columns across all `uniswap_v3_*` tables (query `pg_description`/`information_schema`; mirror the curve comments test).
- [ ] **Step 2:** Run — expect FAIL.
- [ ] **Step 3:** Write `COMMENT ON TABLE/COLUMN` for every table + column, using the `[Type]`/`Roles`/unit-scale style; include the exact units from design §8 (Q64.96, Q128.128, raw native-decimal, hundredths-of-bip, 1.0001^tick).
- [ ] **Step 4:** Run — expect PASS.
- [ ] **Step 5:** Commit: `git commit -m "docs(uniswap-v3): COMMENT every table and column"`

### Task B3: Seed migration — 18 pools + counterparty tokens

**Files:** Create `stl-verify/db/migrations/20260701_100200_seed_uniswap_v3_pools.sql`; test in the integration test file.

**Interfaces:** Produces 15 `token` rows (addresses in design §4, decimals confirmed via a note to verify with `decimals()` at authoring), and 18 `uniswap_v3_pool` rows (address, token0_id/token1_id by natural key, fee, tick_spacing, deploy_block from design §4). Address-equality post-seed assertions (S4 pattern).

- [ ] **Step 1:** Write a test asserting 18 pools seeded with correct fee/tick_spacing/deploy_block and that token0_id/token1_id resolve by `(chain_id, address)`; include the address-equality assertions.
- [ ] **Step 2:** Run — expect FAIL.
- [ ] **Step 3:** Write the seed SQL (INSERT tokens `ON CONFLICT DO NOTHING`; INSERT pools resolving token ids via subselect on `(chain_id, address)`). tick_spacing per fee: 100→1, 500→10, 3000→60, 10000→200.
- [ ] **Step 4:** Run — expect PASS.
- [ ] **Step 5:** Commit: `git commit -m "feat(uniswap-v3): seed 18 active wstETH/stETH pools + tokens"`

### Task B4: Domain entities

**Files:** Create `stl-verify/internal/domain/entity/uniswap_v3.go` + `uniswap_v3_test.go`.

**Interfaces:** Produces `UniswapV3PoolState`, `UniswapV3Swap`, `UniswapV3LiquidityEvent` (Kind mint/burn/collect), `UniswapV3Tick`, `UniswapV3PoolEvent` structs + `Validate()` on each (e.g. tick within int24 range, event kinds valid, amounts non-nil for their kind). No adapter imports.

- [ ] **Step 1:** Write table-driven `Validate()` tests (valid + each invalid case).
- [ ] **Step 2:** Run — expect FAIL. **Step 3:** Implement entities + Validate. **Step 4:** Run — PASS. **Step 5:** Commit `feat(uniswap-v3): domain entities`.

### Task B5: Repository port + postgres adapter

**Files:** Create `stl-verify/internal/ports/outbound/uniswap_v3_repository.go`; `stl-verify/internal/adapters/outbound/postgres/uniswap_v3_repository.go` + integration test.

**Interfaces:**
- Produces: `UniswapV3Repository interface { LoadPools(ctx, chainID int64) ([]UniswapV3PoolRow, error); SaveBlock(ctx, tx pgx.Tx, w UniswapV3BlockWrites) (stateRows int64, err error) }`. `UniswapV3PoolRow{ ID, ProtocolID int64; Address, Token0, Token1 common.Address; Token0Decimals, Token1Decimals int; Fee int; TickSpacing int; DeployBlock int64 }`. `UniswapV3BlockWrites{ States []*entity.UniswapV3PoolState; Swaps []*entity.UniswapV3Swap; LiquidityEvents []...; Ticks []*entity.UniswapV3Tick; PoolEvents []... }`. Tick rows use the `AppendOnChange` helper (Task A4).
- Consumes: `postgres.AppendOnChange`, entities (B4).

- [ ] **Step 1:** Write integration tests: `LoadPools` returns 18 rows with decimals + orientation; `SaveBlock` round-trips each table; tick append-on-change (change/idempotent/reorg-version); state block-hash/version stamping. Mirror `curve_repository_integration_test.go`.
- [ ] **Step 2:** Run — expect FAIL. **Step 3:** Implement port + pgx-batch adapter (mirror `curve_repository.go`). **Step 4:** Run integration — PASS. **Step 5:** Commit `feat(uniswap-v3): repository port + postgres adapter`.

### Task B6: Event decoders (all 9)

**Files:** Create `stl-verify/internal/services/uniswapv3indexer/event_decode.go` + test; `abi.go` (embed the pool ABI event fragments).

**Interfaces:** Produces `DecodeEvents(receipt shared.TransactionReceipt, pool RegisteredPool, chainID, bn int64, ver int, ts time.Time) (DecodedEvents, error)` where `DecodedEvents{ Swaps, LiquidityEvents, PoolEvents []..., Captured []shared.CapturedEvent }`. Uses `shared.DecodeLog`/`shared.LogBelongsTo` (A3). Signed `amount0/1` (int256) decoded correctly. Every log also appended to `Captured` (superset).

- [ ] **Step 1:** Write table-driven decode tests for all 9 events using real log fixtures (topics+data from mainnet txs; capture via `cast tx`/`cast receipt` in the task, or hand-encode). Assert signed amounts, indexed field extraction, tick ranges.
- [ ] **Step 2:** Run — FAIL. **Step 3:** Implement decoders + ABI. **Step 4:** Run — PASS. **Step 5:** Commit `feat(uniswap-v3): decode all 9 pool events`.

### Task B7: Tick handling (touched-set + baseline + ticks() decode)

**Files:** Create `stl-verify/internal/services/uniswapv3indexer/tick.go` + test.

**Interfaces:** Produces `TouchedTicks(evs DecodedEvents) []int32` (union of tickLower/tickUpper from mint/burn/collect, sorted, deduped); `BuildTickCalls(pool, ticks []int32) []outbound.Call` (packs `ticks(int24)`); `DecodeTick(res outbound.Result) (entity.UniswapV3Tick, error)`; `BaselineTicks(ctx, mc Multicaller, pool, blockHash) ([]int32, error)` (walk `tickBitmap(int16)` words across the pool's initialized range, decode set bits → tick indices; bound the scan and log the count). int24 sign handling via `shared` helpers.

- [ ] **Step 1:** Write tests: touched-set union/dedup/sort; int24 negative tick decode; tickBitmap word → tick indices; `DecodeTick` field mapping (liquidityGross/Net signed).
- [ ] **Step 2:** Run — FAIL. **Step 3:** Implement. **Step 4:** Run — PASS. **Step 5:** Commit `feat(uniswap-v3): authoritative tick reads + baseline enumeration`.

### Task B8: State snapshot builder

**Files:** Create `stl-verify/internal/services/uniswapv3indexer/state.go` + test.

**Interfaces:** Produces `SnapshotState(ctx, mc Multicaller, pool RegisteredPool, blockHash common.Hash, bn int64, ver int, ts time.Time) (entity.UniswapV3PoolState, []entity.UniswapV3Tick, error)`. Core calls `AllowFailure:false` (slot0, liquidity, feeGrowthGlobal0/1, protocolFees, balanceOf×2); `observe([window,0])` `AllowFailure:true` → nullable twap_tick (revert = leave NULL, do NOT error — this is the one genuinely-optional read). Price left to read-time (store sqrtPriceX96 raw). Pinned to blockHash (A2).

- [ ] **Step 1:** Write tests: core-read mapping (mock multicaller returns encoded slot0/etc.); a reverted core read → error; observe revert → nil twap_tick, no error; twap tick computed from tickCumulatives delta / window.
- [ ] **Step 2:** Run — FAIL. **Step 3:** Implement. **Step 4:** Run — PASS. **Step 5:** Commit `feat(uniswap-v3): pool-level state snapshot (block-hash pinned)`.

### Task B9: Service orchestration

**Files:** Create `stl-verify/internal/services/uniswapv3indexer/service.go`, `types.go`, `registry.go` + `service_test.go`.

**Interfaces:** Produces `UniswapV3Service` + `func NewUniswapV3Service(UniswapV3ServiceDeps) (*UniswapV3Service, error)` (deps: Pools, Multicaller, Repo, EventWriter, TxManager, ChainID, Logger, Telemetry) and `func (s *UniswapV3Service) BlockHandler() dexconsumer.BlockHandler`. `RegisteredPool{ ID int64; Address, Token0, Token1 common.Address; Token0Decimals, Token1Decimals, Fee, TickSpacing int; DeployBlock int64 }` implements `dexconsumer.SnapshotPool`. Orchestration composes named helpers (prose): `decodeBlockEvents → tracker.DueSet (A1/A2) → snapshotPools(blockHash) → quiet-block skip → buildBlockWrites → persistBlock(one tx)`. No heartbeat snapshot (design §6.1); baseline tick enumeration on first-seen pool. Consumes B5–B8, `dexconsumer.SnapshotTracker`/`ProtocolEventWriter`, block-hash from BlockEvent.

- [ ] **Step 1:** Write `service_test.go` (public API only): construction validates deps; BlockHandler on a block with mixed events persists via a mock repo/eventwriter with the right BlockWrites; quiet block → no tx; error from snapshot → BlockHandler returns error (no ack); first-seen pool triggers baseline. Mock the repo/multicaller/txmanager.
- [ ] **Step 2:** Run — FAIL. **Step 3:** Implement (mirror curve `service.go` structure; single pool class, no capability probe). **Step 4:** Run — PASS (aim 100% service coverage). **Step 5:** Commit `feat(uniswap-v3): block-handler service orchestration`.

### Task B10: Worker entrypoint

**Files:** Create `stl-verify/cmd/workers/uniswap-v3-indexer/main.go` + `main_test.go`.

**Interfaces:** `main()` at top → `run(ctx, args)`; wires `dexbootstrap.ParseConfig("uniswap-v3-indexer", args)` → `Bootstrap` → `repo.LoadPools` → `dexconsumer.ResolveProtocolID(UniswapV3 descriptor)` → `NewProtocolEventWriter` → `NewUniswapV3Service` → `NewBlockProcessor` → `RunLoop`. Mirror `curve-indexer/main.go`.

- [ ] **Step 1:** Write `main_test.go` (integration-only convention): missing SQS_QUEUE_URL / DATABASE_URL / ALCHEMY_API_KEY → `run` returns the expected error. Mirror curve `main_test.go`.
- [ ] **Step 2:** Run — FAIL. **Step 3:** Implement `main.go`. **Step 4:** Run — PASS. **Step 5:** Commit `feat(uniswap-v3): worker entrypoint`.

### Task B11: Telemetry, alerts, runbook

**Files:** Modify `alerts/vector-indexers.yaml` (add `vector-uniswap-v3-indexer` group); `docs/runbooks/vector-indexers.md` (add matching sections). Telemetry via `dextelemetry.NewTelemetry("uniswap_v3", chainID)` (already wired by Bootstrap; confirm the prefix is passed).

- [ ] **Step 1:** Copy the Curve alert group → uniswap_v3: stall (critical, `runbook_url` + section), high error rate (warning + section), processing-but-not-writing-state (warning + section). Metric names `uniswap_v3_*`.
- [ ] **Step 2:** Add matching `## VectorUniswapV3Indexer*` runbook sections (first checks, common causes, recovery query), mirroring the curve sections.
- [ ] **Step 3:** Validate YAML (`cd stl-verify && make lint` or a promtool check if available). Commit `feat(uniswap-v3): alert rules + runbook`.

### Task B12: k8s manifests

**Files:** Create `k8s/base/uniswap-v3-indexer/` (Deployment + ServiceAccount, mirror `curve-indexer` base); add to prod/staging overlays with pinned image tags; dev overlay if applicable.

- [ ] **Step 1:** Copy the curve-indexer base + overlay patches; adjust names/images/queue env. **Step 2:** `kustomize build k8s/overlays/staging` and `.../prod` resolve without error. **Step 3:** Commit `feat(k8s): uniswap-v3-indexer manifests`. (Docker/release consolidation is VEC-329; note that here, don't duplicate it.)

### Task B13: Local real-mainnet validation

- [ ] **Step 1:** `cd stl-verify && make dev-up` with real Alchemy key; deploy the uniswap-v3-indexer pod (or run `run-*` locally against the local cluster).
- [ ] **Step 2:** Let it process live blocks touching the deep wstETH/WETH 0.01% pool; query the DB (db-query skill / psql) and confirm each `uniswap_v3_*` table populates with non-NULL data; verify a swap row, a state row (all core cols non-null), tick rows, and the protocol_event mirror. Confirm baseline tick count is bounded/logged.
- [ ] **Step 3:** Record row/size deltas + any silent-NULL findings in the design doc's validation note. No code commit unless a defect is found (then fix + test first).

---

## Self-review notes (author)

- Spec coverage: events (B6), pool-level state incl balances+TWAP (B8), tick map+baseline (B7/B9), 18 pools (B3), schema (B1/B2), repo (B5), service (B9), worker (B10), telemetry/alerts/runbook (B11), k8s (B12), consolidation (A2–A5), Curve A1/A2 fixes (A2/A1), reorg/pv (B1 triggers + A1 hash), validation (B13). A3 Curve should-fix (sold_id range check) and A6 (get_dy AllowFailure) are Curve-only follow-ups filed separately, not in this plan.
- Types: `RegisteredPool`/`UniswapV3PoolRow`/`UniswapV3BlockWrites`/`DecodedEvents`/`SnapshotPool` names are consistent across B5–B10.
- Placeholder scan: template-reference tasks point at exact curve files; schemas/units come from design §7/§8; no "TBD".
