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
