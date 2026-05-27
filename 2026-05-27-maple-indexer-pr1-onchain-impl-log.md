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
