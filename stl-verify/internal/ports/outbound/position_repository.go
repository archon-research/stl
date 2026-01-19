package outbound

import (
	"context"
	"database/sql"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// PositionRepository defines the interface for user position data persistence.
// This aggregate includes borrower (debt) positions and collateral positions.
type PositionRepository interface {
	// UpsertBorrowers upserts borrower (debt) position records.
	// Each record represents a user's debt in a specific token at a specific block.
	// Note: This method processes in batches. If a batch fails, previous batches remain committed.
	// Use UpsertBorrowersAtomic for all-or-nothing semantics.
	// Conflict resolution: ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version) DO UPDATE
	UpsertBorrowers(ctx context.Context, borrowers []*entity.Borrower) error

	// UpsertBorrowersInTx upserts borrower records using the provided transaction.
	// This allows callers to manage their own transaction boundaries.
	UpsertBorrowersInTx(ctx context.Context, tx *sql.Tx, borrowers []*entity.Borrower) error

	// UpsertBorrowersAtomic upserts all borrower records in a single transaction.
	// If any batch fails, all changes are rolled back.
	UpsertBorrowersAtomic(ctx context.Context, borrowers []*entity.Borrower) error

	// UpsertBorrowerCollateral upserts collateral position records.
	// Each record represents a user's collateral in a specific token at a specific block.
	// Note: This method processes in batches. If a batch fails, previous batches remain committed.
	// Use UpsertBorrowerCollateralAtomic for all-or-nothing semantics.
	// Conflict resolution: ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version) DO UPDATE
	UpsertBorrowerCollateral(ctx context.Context, collateral []*entity.BorrowerCollateral) error

	// UpsertBorrowerCollateralInTx upserts collateral records using the provided transaction.
	// This allows callers to manage their own transaction boundaries.
	UpsertBorrowerCollateralInTx(ctx context.Context, tx *sql.Tx, collateral []*entity.BorrowerCollateral) error

	// UpsertBorrowerCollateralAtomic upserts all collateral records in a single transaction.
	// If any batch fails, all changes are rolled back.
	UpsertBorrowerCollateralAtomic(ctx context.Context, collateral []*entity.BorrowerCollateral) error
}
