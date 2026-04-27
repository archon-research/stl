package outbound

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// PositionRepository defines the interface for user position data persistence.
// This aggregate includes borrower (debt) positions and collateral positions.
type PositionRepository interface {
	// SaveBorrower saves a single borrower position record within an external transaction.
	// Uses append-only semantics: ON CONFLICT DO NOTHING preserves the first write.
	SaveBorrower(ctx context.Context, tx pgx.Tx, b *entity.Borrower) error

	// SaveBorrowers saves multiple borrower (debt) position records in batch using pgx.Batch.
	SaveBorrowers(ctx context.Context, tx pgx.Tx, borrowers []*entity.Borrower) error

	// SaveBorrowerCollateral saves a single collateral position record within an external transaction.
	// Uses append-only semantics: ON CONFLICT DO NOTHING preserves the first write.
	SaveBorrowerCollateral(ctx context.Context, tx pgx.Tx, bc *entity.BorrowerCollateral) error

	// SaveBorrowerCollaterals saves multiple borrower collateral position records in batch.
	SaveBorrowerCollaterals(ctx context.Context, tx pgx.Tx, collaterals []*entity.BorrowerCollateral) error
}
