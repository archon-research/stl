package outbound

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// MaplePositionRepository defines the interface for Maple Finance position data persistence.
// Maple positions use asset symbols instead of token IDs, so they are stored in
// separate tables from the generic borrower/borrower_collateral tables.
//
// The schema is normalized: loan metadata is stored in maple_loan, and both
// maple_borrower and maple_collateral reference it via loan_id.
//
// All methods accept a pgx.Tx so they can participate in a caller-owned transaction.
// The caller is responsible for beginning, committing, and rolling back the transaction.
type MaplePositionRepository interface {
	// SaveLoanSnapshots persists Maple loan metadata snapshots within the provided transaction.
	// Returns a map of loan_address (hex string) -> loan_id for use when persisting borrowers and collateral.
	// Conflict resolution: ON CONFLICT (loan_address, block_number, block_version) DO UPDATE
	SaveLoanSnapshots(ctx context.Context, tx pgx.Tx, snapshots []*entity.MapleLoan) (map[string]int64, error)

	// SaveBorrowerSnapshots persists Maple borrower (debt) position snapshots within the provided transaction.
	// Requires loan_id to be set on each snapshot (obtained from SaveLoanSnapshots).
	// Conflict resolution: ON CONFLICT (loan_id, block_number, block_version) DO NOTHING
	SaveBorrowerSnapshots(ctx context.Context, tx pgx.Tx, snapshots []*entity.MapleBorrower) error

	// SaveCollateralSnapshots persists Maple collateral position snapshots within the provided transaction.
	// Requires loan_id to be set on each snapshot (obtained from SaveLoanSnapshots).
	// Conflict resolution: ON CONFLICT (loan_id, block_number, block_version) DO NOTHING
	SaveCollateralSnapshots(ctx context.Context, tx pgx.Tx, snapshots []*entity.MapleCollateral) error
}
