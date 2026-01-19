package outbound

import (
	"context"
	"database/sql"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// PositionRepository defines the interface for user position data persistence.
// This aggregate includes borrower (debt) positions and collateral positions.
type PositionRepository interface {
	// UpsertBorrowers upserts borrower (debt) position records atomically.
	// All records are inserted in a single transaction - if any batch fails, all changes are rolled back.
	// Conflict resolution: ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version) DO UPDATE
	UpsertBorrowers(ctx context.Context, borrowers []*entity.Borrower) error

	// UpsertBorrowersInTx upserts borrower records using the provided transaction.
	// This allows callers to manage their own transaction boundaries for complex operations.
	UpsertBorrowersInTx(ctx context.Context, tx *sql.Tx, borrowers []*entity.Borrower) error

	// UpsertBorrowerCollateral upserts collateral position records atomically.
	// All records are inserted in a single transaction - if any batch fails, all changes are rolled back.
	// Conflict resolution: ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version) DO UPDATE
	UpsertBorrowerCollateral(ctx context.Context, collateral []*entity.BorrowerCollateral) error

	// UpsertBorrowerCollateralInTx upserts collateral records using the provided transaction.
	// This allows callers to manage their own transaction boundaries for complex operations.
	UpsertBorrowerCollateralInTx(ctx context.Context, tx *sql.Tx, collateral []*entity.BorrowerCollateral) error
}
