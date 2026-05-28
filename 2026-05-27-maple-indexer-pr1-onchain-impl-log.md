# Maple Indexer PR1 — Implementation Log

Plan: `/Users/andrius/workspace/plans/stl-docs/2026-05-27-maple-indexer-pr1-onchain.md`
Branch: `maple-indexer`
Worktree: `/Users/andrius/workspace/stl-worktrees/maple-indexer/`

---

## Task 1 — Migration `20260527_100000_create_maple_tables.sql`

**File:** `stl-verify/db/migrations/20260527_100000_create_maple_tables.sql`

### What was done
- Added seed rows: 4 `protocol` rows (`maple-syrup-v1`, `maple-poolv2`, `maple-otl`, `maple-ftl`), 1 `token` row for `syrupUSDC` (`syrupUSDT` already seeded by `20260305_100000_add_aave_v3_oracle_feeds.sql`), 2 `receipt_token` rows (`syrupUSDC→USDC`, `syrupUSDT→USDT`).
- Created `maple_vault` registry table + 4 indexes. Seeded SyrupUSDC + SyrupUSDT rows (lookup by address).
- Created `maple_vault_state` hypertable + compression policy (2 days) + tiering policy (1 year, best-effort), segment by `maple_vault_id`. **Includes `processing_version`+`build_id` from inception**.
- Created `maple_vault_position` hypertable + same policy pattern, segment by `(maple_vault_id, user_id)`.
- Created two processing-version trigger functions + triggers (mirrors `assign_processing_version_morpho_vault_state` / `_morpho_vault_position`).
- Registered in `migrations` table.

### Decisions & rationale
- **PK order:** `(entity_id, block_number, block_version, processing_version, timestamp)` — matches morpho post-`20260410_130000_alter_constraints.sql`. Plan had `(…, timestamp, processing_version)`; I aligned with morpho convention.
- **Auditability columns baked in from inception** (NOT NULL DEFAULT 0) rather than added via follow-on migrations, since the convention is now firmly established. Avoids a 7-file migration dance just for one table pair.
- **No FK on `build_id` to `build_registry(id)`** — plan suggested REFERENCES; morpho doesn't. Followed morpho.
- **`receipt_token` schema** — plan inserted `(chain_id, receipt_token_id, underlying_token_id)` which doesn't match actual schema. Real columns: `(chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol, ...)`. UNIQUE constraint `(chain_id, receipt_token_address)` (set by `20260319_100000_update_receipt_token.sql`).
- **`token` insert** — plan inserted `name` + `is_native` columns; neither exists. Real schema: `(chain_id, address, symbol, decimals, created_at_block, updated_at, metadata)`. Inserted only `(chain_id, address, symbol, decimals)`.
- **`syrupUSDT` already seeded** by `20260305_100000_add_aave_v3_oracle_feeds.sql` (symbol `'syrupUSDT'`, lowercase 's'). Used address-based lookup in `maple_vault` + `receipt_token` to be casing-immune.
- **Pool address for SyrupUSDT** = zero placeholder until confirmed in Task 17 smoke test. Follow-on migration updates (never edit this file).

### Verification
- Ran `go test -tags integration ./db/migrator/ -run TestMigrator_ApplyAll|TestMigrator_VerifySchema` — PASS, all 56 migrations apply against fresh TimescaleDB 2.25.1-pg17 container.
- Checksum: `3fc98d4c`.

### Gotchas
1. First attempt used `ON CONFLICT (protocol_id, underlying_token_id)` for `receipt_token` — failed because that unique constraint was dropped in `20260319_100000`. Replaced with `(chain_id, receipt_token_address)`.
2. Token table has no `name` or `is_native` columns — plan's seed INSERT was wrong; trimmed to actual schema.

### State of codebase
- Migration applied successfully; tables ready for entities/repo (Tasks 2-7).
- No env vars added.

---

## Tasks 2-7 — Domain entities + ABI + Port + Postgres adapter

### Task 2: `MapleVault` entity
- File: `internal/domain/entity/maple_vault.go` (+ `_test.go`, 8 test cases).
- Used `int64` chainID (matches morpho's MorphoVault), not plan's `int`. Dropped plan's `_unusedID` argument slot.

### Task 3: `MapleVaultState` entity
- File: `internal/domain/entity/maple_vault_state.go` (+ `_test.go`, 7 test cases).
- **Dropped `BuildID` from entity** — morpho convention puts build_id on the repository, not the domain entity. Repo passes `int(r.buildID)` at insert time.
- Used `BlockTimestamp` (morpho name) and `BlockVersion int` (not int32).
- Added `WithPrices()` helper for the nullable USD slots.

### Task 4: `MapleVaultPosition` entity
- File: `internal/domain/entity/maple_vault_position.go` (+ `_test.go`, 7 test cases).
- Same pattern as state: no BuildID field, BlockTimestamp name, `BlockVersion int`.

### Task 5: Syrup ABI accessors
- File: `internal/pkg/blockchain/abis/syrup_vault_abi.go` (+ `_test.go`).
- **Departed from plan:** plan suggested `//go:embed` JSON files; repo convention uses inline strings via `ParseABI(...)` (see `metamorpho_abi.go`, `sparklend_abi.go`). Followed repo convention — no JSON files created.

### Task 6: `MapleRepository` outbound port
- File: `internal/ports/outbound/maple_repository.go`.
- Uses `pgx.Tx` directly (matches morpho port), not the plan-suggested `outbound.Tx` (no such type exists in this repo).
- API: `GetAllVaults`, `SaveVaultState` (single), `SaveVaultPositions` (batch).

### Task 7: Postgres `MapleRepository` adapter
- Files: `internal/adapters/outbound/postgres/maple_repository.go` + integration test (7 cases, all passing).
- Constructor takes `buildregistry.BuildID` (an `int` alias) — repo writes `int(r.buildID)` into `build_id` column.
- Uses `bigIntToNumeric` (existing helper) + nullable `*string` for `underlying_price_usd` / `syrup_price_usd`.
- Batch insert via `pgx.Batch` for positions.
- ON CONFLICT clause references all PK columns: `(maple_vault_id, block_number, block_version, processing_version, timestamp)` — matches morpho convention post-`20260410_130000_alter_constraints.sql`.

### Decisions / departures from plan (cumulative)
| Plan said | Actually did | Why |
|---|---|---|
| Entity carries `BuildID` | Repo carries `BuildID` | Mirrors morpho — keeps entity ignorant of persistence concerns |
| Use `//go:embed` JSON | Inline strings via `ParseABI` | Repo convention — no embed files anywhere in `abis/` |
| `outbound.Tx` | `pgx.Tx` | No such marker type in this repo |
| `_unusedID int64` slot in `NewMapleVault` | Removed | Awkward; mirrored MorphoVault constructor instead |
| Migration PK ends in `(timestamp, processing_version)` | `(processing_version, timestamp)` | Matches morpho PK ordering post-alter |

### Verification snapshot
- Unit tests: 23 entity tests PASS, 2 ABI smoke tests PASS.
- Integration tests: 7 maple repo + 1 migrator schema PASS against fresh TimescaleDB container.

### State of codebase
- Migrations applied; entities + port + adapter ready.
- Next: service layer (Tasks 8-13), worker cmd (14), Dockerfile/Makefile (15), k8s (16).
- No new env vars. No new global helpers.

---

## Task 8 — Service Config + Telemetry skeleton

**Files:**
- `stl-verify/internal/services/maple_indexer/types.go`
- `stl-verify/internal/services/maple_indexer/telemetry.go`
- `stl-verify/internal/services/maple_indexer/types_test.go`

### What was done
- `types.go`: `mapleSyrupDeployBlocks` map (chain 1 → 20231245) with `MapleSyrupDeployBlock()` accessor that errors on unknown chains. `Config` wraps `shared.SQSConsumerConfig` with optional `*Telemetry` field.
- `telemetry.go`: full OTel surface mirroring morpho's `telemetry.go`. Counters: `blocksProcessed`, `vaultStateWrites`, `positionWrites`, `rpcCallsTotal`, `errorsTotal`. Histograms: `blockDuration`, `receiptDuration`, `rpcDuration`. All recorders are nil-safe so unit tests can pass nil Telemetry.
- 4 unit tests: deploy-block lookup (3 sub-cases), config defaults, nil-safe recorders, full constructor sanity.

### Decisions / departures from plan
- **Richer telemetry than plan's minimal `MessagesProcessed/VaultStateWrites/PositionWrites/BlockLag`.** Mirrored morpho's pattern (tracer + spans + dedicated recorder methods) for consistency — the service in Task 13 will need spans to wrap block/receipt processing, and adding them piecemeal later would just churn this file.
- Renamed plan's `MessagesProcessed` to `blocksProcessed` since `sqsutil.RunLoop` is the message-level handler; what we record here is at the block-event granularity.
- Plan suggested `ConfigDefaults() Config` returning `Config{SQSConsumerConfig: shared.SQSConsumerConfigDefaults()}` — kept verbatim.
- Plan's `SyrupDeployBlock(chainID int64) (int64, bool)` boolean-OK signature changed to `MapleSyrupDeployBlock(chainID int64) (int64, error)` — matches morpho's `MorphoBlueDeployBlock` error-return pattern.

### Gotchas
- None — straightforward port of morpho's pattern.

### State of codebase
- Package `maple_indexer` compiles, 4 tests pass.
- Next: Task 9 (event_extractor.go). Use the syrup events ABI already in `internal/pkg/blockchain/abis/syrup_vault_abi.go`. Topic hashes only — full Morpho-style event-decoding is overkill for Syrup because the service only needs to know which users were touched, not the parsed args.


## Task 9 — EventExtractor

**Files:**
- `stl-verify/internal/services/maple_indexer/event_extractor.go`
- `stl-verify/internal/services/maple_indexer/event_extractor_test.go` (13 cases)

### What was done
- `EventExtractor` caches the keccak topic-hashes for Syrup `Deposit`, `Withdraw`, `Transfer`. No full ABI decoding — Syrup indexing only needs to know *which* users were touched.
- `IsRelevant(vault, log)` — `vault address match` AND `recognised topic[0]`.
- `ExtractTouchedAddresses(vault, logs) -> (map[Address]struct{}, bool)`:
  - **Deposit:** `Topics[1]=sender, Topics[2]=owner` → refresh both.
  - **Withdraw:** `Topics[2]=receiver, Topics[3]=owner` → refresh both. **Sender (Topics[1]) intentionally NOT refreshed** — that slot is `msg.sender`, which on router-style withdrawals is a contract, not a position holder.
  - **Transfer:** `Topics[1]=from, Topics[2]=to` → refresh both, skipping the zero address (mint/burn counter-party).
- Zero-address filter is global: any topic decoded to `0x0` is dropped.
- Map-based return naturally dedupes across multiple logs touching the same user.

### Decisions / departures from plan
- Plan's test imports `core/types.Log`. Reality uses `shared.Log` (JSON-shaped, `Address`/`Topics`/`Data` as strings) — that's what `service.go` parses receipts into. Tests use `shared.Log` accordingly.
- **Withdraw extractor narrows refresh set to (receiver, owner), excluding sender.** Plan said refresh "owner (and receiver if non-zero)". The ERC-4626 `Withdraw` event has `sender = msg.sender`, `receiver = assets recipient`, `owner = shares burner`. Router-style withdrawals (e.g. Maple's PoolManager calling the vault) have `sender` as a contract address that holds no Syrup shares; refreshing it is wasted multicall budget.
- Used `common.HexToHash(topicHex).Bytes()` then `common.BytesToAddress` for safe topic→address conversion (left-padded 32-byte topic → 20-byte address).
- Added explicit handling for malformed logs (matching topic but missing indexed slots) → contributes 0 users but `relevant=true` stays so the caller can still write a vault_state row if needed.

### Gotchas
- The Syrup ABI sometimes ships with named-parameter variations across deploys, but the canonical ERC-4626 signatures match what's in `syrup_vault_abi.go`. Stuck to those names; if a vault's deploy bytecode emits a different parameter ordering, the topic hash would differ and `IsRelevant` would correctly reject it.

### State of codebase
- 13 unit tests pass (covers happy path for all 3 event types + edge cases).
- Next: Task 10 (vault_registry.go) — mirror morpho's but drop `notVaults` since Maple registry is static (DB-seeded, no on-chain discovery).


## Task 10 — VaultRegistry + shared testhelpers_test.go

**Files:**
- `stl-verify/internal/services/maple_indexer/vault_registry.go`
- `stl-verify/internal/services/maple_indexer/vault_registry_test.go` (6 cases)
- `stl-verify/internal/services/maple_indexer/testhelpers_test.go` (shared stubs — created early; plan put this in Task 12 but it's needed now)

### What was done
- `VaultRegistry` mirrors morpho's surface (`LoadFromDB`, `IsKnownVault`, `GetVault`, `All`, `Count`) but **drops** `notVaults` cache, `MarkNotVault`, `RegisterVault`. Maple's registry is static (DB-seeded only) — no on-chain discovery means no negative caching and no runtime registration. Restart picks up new seed migrations.
- `All()` returns a snapshot slice copy so callers iterate without holding the registry RWMutex.
- `testhelpers_test.go` ships `mapleRepoStub` (satisfies `outbound.MapleRepository`), `multicallStub` (satisfies `outbound.Multicaller`), `encodeUint256`/`encodeUint8` ABI encoding helpers.

### Decisions / departures from plan
- **Plan put shared helpers in Task 12.** Pulled them forward to Task 10 because Task 11 (blockchain_service tests) and Task 13 (service tests) both need them; building duplicate stubs in 3 places and consolidating later would be wasted work. `testhelpers_test.go` is package-internal (suffix `_test.go`), no production code touched.
- **`multicallStub.Calls` and `BlockNumbers` capture each invocation** so tests can assert call shape (used heavily in Task 11).
- `RegisterVault` is **intentionally absent** — adding it would invite drift between in-memory state and the DB. Maple's vault set changes via a new seed migration, not at runtime.

### Gotchas
- Initial draft of `vault_registry_test.go` tried to inline a partial repo stub that didn't satisfy `outbound.MapleRepository` (missing pgx.Tx-typed methods). Switched to the shared `mapleRepoStub` which fully implements the interface (compile-time `var _ outbound.MapleRepository = (*mapleRepoStub)(nil)` check guards this).

### State of codebase
- 6 registry tests pass.
- Shared stubs ready for Tasks 11 + 13.
- Next: Task 11 (blockchain_service.go) — multicall wrapper for FetchVaultState + FetchUserPositions.


## Task 11 — BlockchainService (multicall wrapper)

**Files:**
- `stl-verify/internal/services/maple_indexer/blockchain_service.go`
- `stl-verify/internal/services/maple_indexer/blockchain_service_test.go` (9 cases)

### What was done
- `BlockchainService` wraps `outbound.Multicaller` + Syrup views ABI. No state across blocks; safe for shared use.
- `NewBlockchainService` pre-packs the no-argument view call data once (`totalAssets`, `totalSupply`, `decimals`). `convertToAssets` is packed per call because the share-unit argument is constant but the per-user variant takes a dynamic balance.
- `FetchVaultState` — one multicall with 4 calls: totalAssets / totalSupply / convertToAssets(`shareUnit=1e6`) / decimals. Returns `VaultStateRaw{TotalAssets, TotalSupply, SharePrice, Decimals}`.
- `FetchUserPositions` — two sequential multicalls:
  1. `balanceOf(user)` for each user → shares.
  2. `convertToAssets(shares)` for each user → assets.
  Sequential because the second batch's args depend on the first's results.
- All decode helpers verify `Result.Success` and `len(ReturnData) > 0` before unpacking; reverted calls produce a typed error.
- Empty user slice short-circuits to empty map without invoking multicaller.

### Decisions / departures from plan
- **Plan suggested a custom `multicall.Caller` interface.** Reality uses the repo's existing `outbound.Multicaller` (signature `Execute(ctx, calls, blockNumber)` returning `[]Result`). Followed repo convention.
- **shareUnit hard-coded to 1e6** with an explicit comment. SyrupUSDC + SyrupUSDT both have 6 decimals; the decimals() read is captured in `VaultStateRaw.Decimals` for future expansion. If a non-6-decimal Syrup vault ships, switch to `10^decimals` per-call (one extra call per vault).
- **Pre-packed call data** for the no-argument views — small optimisation that matters when the service runs ~80k blocks/day on mainnet and packs the same selector each time.
- **Test stub `multicallStub` captures `Calls` and `BlockNumbers`** to let tests assert call shape and block-tag propagation. Useful in Task 13 too.

### Gotchas
- `viewABI.Unpack(method, raw)` requires the method name to match an ABI entry; the decoder uses the method name to look up the output schema, NOT to validate the input selector. So `decodeUint256("totalAssets", balanceOfResult)` would technically work because both return a single uint256 — but the explicit per-method calls keep error messages traceable.
- `Result.Success=false` with empty `ReturnData` typically indicates a multicall sub-call revert (not an RPC error). We treat that as a hard error so the SQS message redelivers.

### State of codebase
- 9 unit tests pass (constructor, vault state happy path, error paths, two-batch flow, block-tag propagation).
- 38 total tests in `services/maple_indexer` package.
- Next: Task 13 (service.go) — wires registry + extractor + blockchain + repo + tx_manager.


## Task 13 — Service (SQS consumer loop + per-message handler)

**Files:**
- `stl-verify/internal/services/maple_indexer/service.go`
- `stl-verify/internal/services/maple_indexer/service_test.go` (15 cases)

### What was done
- `Service` wires Config + ports + auxiliary components. NewService validates every dep (consumer, cache, multicaller, txManager, userRepo, mapleRepo) and rejects unsupported chains.
- `Start(ctx)`: cancellable child ctx → registry.LoadFromDB → `sqsutil.RunLoop` in a goroutine. Returns immediately.
- `Stop()`: nil-safe cancel.
- `processBlockEvent` → `fetchAndProcessReceipts` (with tracing + duration recording).
- `fetchAndProcessReceipts`:
  1. GetReceipts from cache (nil receipts = hard error → SQS redeliver).
  2. Unmarshal `[]shared.TransactionReceipt`.
  3. `touchedVaults(receipts)` flattens logs, walks registered vaults, calls `ExtractTouchedAddresses` per vault.
  4. **Sorts vault addresses + user addresses by bytes** so concurrent indexer instances acquire user/vault locks in identical order (defence-in-depth against future deadlocks; see ADR-0002 §3 cited in morpho's code).
  5. For each touched vault, runs `indexVault` and collects errors via `errors.Join` so one vault's failure doesn't shadow another.
- `indexVault`:
  1. FetchVaultState (one multicall).
  2. FetchUserPositions (two multicalls).
  3. Build entity.MapleVaultState.
  4. **One DB tx per vault per block**: SaveVaultState → buildPositionEntities (per-user GetOrCreateUser + entity construction) → SaveVaultPositions.
  5. Telemetry: VaultStateWrite + PositionWrites.
- `buildPositionEntities`: sorted users → user upsert inside the open tx → entity construction.

### Test coverage (15 service-level cases)
- Constructor: nil-dep rejection (6 cases inlined), unsupported chain.
- Receipts cache: nil receipts errors, propagated cache errors.
- No relevant logs → no writes, no multicall invocation.
- Deposit happy path: 1 vault_state, 2 position rows in sorted user order, correct big.Int decoding.
- Error propagation: vault-state RPC error, vault-state save error, user repo error.
- Dedup: user touched by Deposit + Transfer → one position row.
- One-tx-per-vault invariant (counts WithTransaction calls).
- `touchedVaults` direct: ignores logs from non-registered addresses.
- Stop pre-Start: no panic.
- Sanity: user A/B/C byte ordering matches the sort assumption in tests.

### Decisions / departures from plan
- **No `protocol_event` audit log writes.** Morpho writes every event as a protocol_event row; the maple plan and spec deliberately omit this for PR1 — the Syrup index goal is per-block state + positions, not a full event log. Add later via a new repo method if needed.
- **No vault discovery branch.** Registry is static; if a log addresses a non-registered vault, `touchedVaults` simply ignores it. Morpho's discovery + probe machinery is unnecessary here.
- **`sortedUsers + vault sort` even though only one Syrup vault is touched per block today.** Future-proofs against multi-vault transactions (e.g. a router that mints into SyrupUSDC and burns SyrupUSDT in one tx) and matches morpho's defensive sort convention.
- **`fetchAndProcessReceipts` does NOT pre-walk for any discovery.** Morpho's pre-walk pattern is V1/V1.1 vault discovery via the Morpho Blue path — irrelevant here.
- **Construction without DB**: registry is loaded in `Start()`, not `NewService()`, so unit tests can construct a Service and inject vaults via direct `registry.LoadFromDB` call.
- **`userRepoStub` lives in service_test.go** (not testhelpers) because it's only used by service tests. Kept testhelpers focused on cross-test stubs.

### Gotchas
- **Initial test file had a vestigial `cases := []struct{...}` table** from a refactor — compile error in the cases.func anonymous-struct type vs concrete signature. Removed and replaced with inline nil checks (simpler and equivalent coverage).
- **First instinct was to use `testutil.MockUserRepository`** but its default `GetOrCreateUser` returns `(1, nil)` regardless of address, which would make the per-user position assertions meaningless (all positions get user_id=1). Wrote `userRepoStub` with proper address-keyed ID assignment.
- `Service.cancel` starts nil → `Stop()` must nil-check before calling, otherwise unit tests panic.
- `errors.Join(nil)` returns nil so the `errs []error` slice + `errors.Join(errs...)` pattern is safe when no errors occur.

### State of codebase
- 47 tests in `services/maple_indexer` pass, race detector clean.
- `go build ./...` clean for the entire module.
- Next: Task 14 (`cmd/workers/maple-indexer/main.go`) — wire the service against real adapters (postgres, redis, alchemy multicall, SQS, telemetry).


## Task 14 — `cmd/workers/maple-indexer` (main.go + tests)

**Files:**
- `stl-verify/cmd/workers/maple-indexer/main.go`
- `stl-verify/cmd/workers/maple-indexer/main_test.go` (19 parseConfig sub-cases)
- `stl-verify/cmd/workers/maple-indexer/main_integration_test.go` (2 cases, build tag `integration`)

### What was done
- Copied morpho-indexer cmd structure verbatim; swapped `morpho-indexer` → `maple-indexer` and `morpho_indexer` → `maple_indexer` everywhere.
- Service wiring follows Task 13's `NewService(config, consumer, cache, multicaller, txManager, userRepo, mapleRepo)` signature — **dropped** `protocolRepo`, `tokenRepo`, `eventRepo`, `receiptTokenRepo` from the call site (Syrup needs none of them).
- Telemetry: `maple_indexer.NewTelemetry()` returned from `services/maple_indexer/telemetry.go`; OTEL service name set to `"maple-indexer"` for spans/metrics.
- Repositories constructed: `txManager`, `userRepo`, `mapleRepo`. Build registry passes `buildReg.BuildID()` into `NewMapleRepository` exactly as morpho does.
- Integration tests reuse `testutil.{StartTimescaleDBForMain, StartRedisForMain, StartLocalStackForMain, StartMockSQS, SetupTestSchema}` — no new helpers invented.

### Decisions / departures from plan
- Plan's "omit eventRepo if not in signature" branch hit. Task 13's `NewService` takes 7 args (no event/protocol/token/receiptToken). Verified by reading `service.go` constructor signature before writing the call site.
- The dev binary `stl-verify/maple-indexer` was a stray Mach-O from a manual `go build .` run; removed before running tests so `go test ./cmd/workers/maple-indexer/` resolves the package (not the binary).

### Test results
- Unit (`TestParseConfig`): 19 sub-cases PASS.
- Integration (`TestRunIntegration_BadConnectionConfig`, `TestRunIntegration_StartupAndShutdown`): both PASS, 33.8s total (TimescaleDB + LocalStack + Redis containers spin up via `TestMain`).
- Full suite `go test ./...` clean — no regressions.

### Gotchas
- `go test ./cmd/workers/maple-indexer/` initially balked because cwd contained a stray `maple-indexer` binary (from a one-off `go build .` debug step). `go test` resolves `./cmd/workers/maple-indexer/` against the package import path, but the bash session's working-directory aliasing made the binary visible. Cleanup: `rm -f stl-verify/maple-indexer` before re-running tests.
- `lifecycle.Run(ctx, logger, service)` is the same lifecycle helper morpho uses — `Service` already satisfies `Start(ctx) error` + `Stop() error`, no adapter glue needed.

### State of codebase
- Worker entry point ready. `make run-maple-indexer` not yet added (Task 15).
- No new env vars beyond morpho's set (ALCHEMY_API_KEY, REDIS_ADDR, DATABASE_URL, AWS_SQS_QUEUE_URL, S3_BUCKET, DEPLOY_ENV, optional CHAIN_ID / SQS_WAIT_TIME / SQS_VISIBILITY_TIMEOUT).
- Next: Task 15 (Dockerfile.maple-indexer + Makefile targets).


## Task 15 — Dockerfile + Makefile targets

**Files:**
- `stl-verify/Dockerfile.maple-indexer` (new)
- `stl-verify/Makefile` (modified)

### What was done
- `Dockerfile.maple-indexer`: identical to `Dockerfile.morpho-indexer` with binary/path renames. ARM64 cross-compile, non-root runtime user, static binary, no exposed ports (background worker).
- `Makefile`:
  - `.PHONY`: registered `run-maple-indexer`, `_docker-release-maple-indexer-internal`, and the 6 `docker-{build,push,release}-maple-indexer{,-staging}` targets.
  - `run-maple-indexer`: mirror of `run-morpho-indexer` (`set -a && . cmd/workers/maple-indexer/.env...`).
  - `dev-up`: appended `maple-indexer` to the Alchemy-dependent DEPS list at line 363; updated the two help/warning strings + `dev-up-workers` MISSING string + Alchemy-workers deploy banner.
  - `_dev-up-alchemy-workers`: `build-if-needed stl-maple-indexer:local` + rollout-status loop now waits for `maple-indexer` deployment.
  - `dev-env`: new `cmd/workers/maple-indexer/.env` stanza pointing at `stl-ethereum-maple-indexing.fifo` SQS queue + chain_id=1.
  - `docker-release-all`: added `_docker-release-maple-indexer-internal` between morpho and allocation-tracker.
  - `kind-load-workers`: added `stl-maple-indexer:local` to the IMAGES list.
  - `kind-deploy-workers`: added `deployment/maple-indexer` to the rollout-restart chain.
  - Added the full ARM64 build/push/release block + sentinelstaging shortcuts (mirrors morpho block ~70 lines).

### Decisions / departures from plan
- Plan said to add `docker-release-maple-indexer-staging` etc. as standalone phony targets. I followed morpho's actual pattern: a real block (ECR_REPO_MAPLE_INDEXER + docker-build/push/release with LOCAL guard) followed by a "Sentinelstaging shortcuts" section. Matches morpho 1:1 so the dev-up flow finds the image identically.
- Plan said the dev-up DEPS list lives around line 362; verified by `grep` — exact match. Same with the `for dep in …` rollout list at line 423.
- Did NOT touch `_docker-release-internal _docker-release-backup-internal …` line 10 of `.PHONY` — already added `_docker-release-maple-indexer-internal` there.

### Verification
- `make -n docker-build-maple-indexer LOCAL=1` → resolves to `docker build … -t stl-maple-indexer:local -f Dockerfile.maple-indexer .`
- `make -n run-maple-indexer` → resolves to `echo + set -a && . cmd/workers/maple-indexer/.env && go run ./cmd/workers/maple-indexer`
- `make docker-build-maple-indexer LOCAL=1` actually executed → `stl-maple-indexer:local` image produced (ARM64 cross-compile, 7s go build inside Alpine).

### Gotchas
- `docker buildx build … --load .` produces a single-arch image (linux/arm64) on Apple Silicon; on AMD hosts the LOCAL fallback in this Makefile (`docker build` without buildx) auto-picks the host arch — fine because LOCAL=1 is only the kind dev workflow, where kind nodes match host arch.
- `.PHONY` is split across multiple lines; the new entries had to be added to **two** different continuation lines (the run-* line at top, and the docker-release-internal helper line). Forgetting one of the two would silently keep `_docker-release-maple-indexer-internal` from being treated as phony and could break re-runs.

### State of codebase
- `dev-up` will now expect `maple-indexer` k8s deployment to exist. Next task (16) creates it. **Until then, `make dev-up` will hang on the rollout-status wait** — running Task 16 before exercising dev-up is recommended.
- No env vars added beyond what the morpho worker already needs.
- Next: Task 16 (k8s manifest + overlay patches).


## Task 16 — Kubernetes manifest

**Files:**
- `stl-verify/k8s/apps/workers/maple-indexer.yaml` (new)

### What was done
- Copied `morpho-indexer.yaml` verbatim. Renamed every `morpho-indexer` token to `maple-indexer`. Changed `AWS_SQS_QUEUE_URL` to `stl-ethereum-maple-indexing.fifo`. Single-container Deployment, replicas=1, `localhost/stl-maple-indexer:local` image with `imagePullPolicy: Never` (kind-loaded), pulls env from existing `stl-config` ConfigMap + `stl-secrets` Secret.
- Validated: `kubectl --dry-run=client apply -f stl-verify/k8s/apps/workers/maple-indexer.yaml` → `deployment.apps/maple-indexer created (dry run)`.

### Decisions / departures from plan
- **No `k8s/overlays/*` edits.** The plan assumed a Kustomize layout with `k8s/overlays/{prod,staging}/kustomization.yaml`. This repo has no `overlays/` directory at all (verified via `find stl-verify/k8s -type d`). Workers are plain single-file Deployments in `k8s/apps/workers/`. Prod/staging image-tag rotation is handled outside this repo via the `deploy(staging|prod): update image tags …` automation against a separate config repo.
- **No infra/terraform changes.** SQS FIFO queue `stl-ethereum-maple-indexing.fifo` + matching SNS subscription must exist before prod rollout; that lives in the separate infra repo. Will flag in the PR description (Task 18).
- **No kustomize validation.** Used `kubectl --dry-run=client apply` instead — same intent, matches the repo's actual deployment style.

### Verification
- Manifest validates via `kubectl --dry-run=client apply`.
- `make dev-up` should now find the deployment when the `_dev-up-alchemy-workers` rollout-status loop hits `deployment/maple-indexer`.

### State of codebase
- Worker stack code-complete. End-to-end smoke (Task 17) is optional verification — skipped in this session because there's no live kind cluster running and `make dev-up` requires Docker plus real ALCHEMY_API_KEY for full workers spin-up. The structural pieces (image build, manifest validation) are confirmed.
- **Infra dependency to flag in PR:** the FIFO queue `stl-ethereum-maple-indexing.fifo` + matching SNS topic subscription must be provisioned in the infra repo before prod deploy.
- Next: Task 17 (e2e smoke — defer/skip), Task 18 (reviewer pass + PR open).



