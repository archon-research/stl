# Branch: toreluntang/maple-indexer

## What
SQS consumer that indexes Maple Finance borrower positions and collateral from the Maple GraphQL API, triggered by block events.

## Current State
- Maple indexer service is functional: consumes SQS block events, fetches active loans from Maple API, and persists borrower/collateral snapshots.
- **Maple data is isolated into its own tables** (`maple_borrower`, `maple_collateral`) rather than sharing `borrower`/`borrower_collateral` with SparkLend/Aave. This was done because Maple uses symbol-only assets (e.g. "BTC", "XRP") with no on-chain address, which doesn't fit the `token` table's `chain_id + address` model.
- A prior attempt at a shared `protocol_asset` indirection layer was introduced and then reverted (commits `64a0221` / `43be307`) due to excessive cross-cutting complexity.
- SparkLend code is untouched by this branch.

## Key Files
- `stl-verify/cmd/maple-indexer/main.go` - entry point
- `stl-verify/internal/services/maple_indexer/service.go` - core service
- `stl-verify/internal/domain/entity/maple_borrower.go` / `maple_collateral.go` - domain entities
- `stl-verify/internal/ports/outbound/maple_position_repository.go` - repository port
- `stl-verify/internal/adapters/outbound/postgres/maple_position_repository.go` - postgres adapter
- `stl-verify/internal/adapters/outbound/maple/client.go` - GraphQL client
- `stl-verify/db/migrations/20260303_100000_create_maple_tables.sql` - migration
- `docs/plans/maple-isolation-plan.md` - design rationale
