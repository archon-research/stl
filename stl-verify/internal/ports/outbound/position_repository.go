package outbound

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// PositionRepository defines the interface for user position data persistence.
// This aggregate includes borrower (debt) positions and collateral positions.
type PositionRepository interface {
	// UpsertBorrowers upserts borrower (debt) position records atomically.
	// All records are inserted in a single transaction - if any batch fails, all changes are rolled back.
	// Conflict resolution: ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version) DO UPDATE
	UpsertBorrowers(ctx context.Context, borrowers []*entity.Borrower) error

	// UpsertBorrowersTx upserts borrower (debt) position records within an existing transaction.
	UpsertBorrowersTx(ctx context.Context, tx pgx.Tx, borrowers []*entity.Borrower) error

	// UpsertBorrowerCollateral upserts collateral position records atomically.
	// All records are inserted in a single transaction - if any batch fails, all changes are rolled back.
	// Conflict resolution: ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version) DO UPDATE
	UpsertBorrowerCollateral(ctx context.Context, collateral []*entity.BorrowerCollateral) error

	// UpsertBorrowerCollateralTx upserts collateral position records within an existing transaction.
	UpsertBorrowerCollateralTx(ctx context.Context, tx pgx.Tx, collateral []*entity.BorrowerCollateral) error
}
