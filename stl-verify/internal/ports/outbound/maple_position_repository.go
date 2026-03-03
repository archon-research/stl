package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// MaplePositionRepository defines the interface for Maple Finance position data persistence.
// Maple positions use asset symbols instead of token IDs, so they are stored in
// separate tables from the generic borrower/borrower_collateral tables.
//
// The schema is normalized: loan metadata is stored in maple_loan, and both
// maple_borrower and maple_collateral reference it via loan_id.
type MaplePositionRepository interface {
	// SaveLoanSnapshots persists Maple loan metadata snapshots atomically.
	// Returns a map of loan_address (hex string) -> loan_id for use when persisting borrowers and collateral.
	// All records are inserted in a single transaction - if any record fails, all changes are rolled back.
	// Conflict resolution: ON CONFLICT (loan_address, block_number, block_version) DO UPDATE
	SaveLoanSnapshots(ctx context.Context, snapshots []*entity.MapleLoan) (map[string]int64, error)

	// SaveBorrowerSnapshots persists Maple borrower (debt) position snapshots atomically.
	// Requires loan_id to be set on each snapshot (obtained from SaveLoanSnapshots).
	// All records are inserted in a single transaction - if any record fails, all changes are rolled back.
	// Conflict resolution: ON CONFLICT (loan_id, block_number, block_version) DO UPDATE
	SaveBorrowerSnapshots(ctx context.Context, snapshots []*entity.MapleBorrower) error

	// SaveCollateralSnapshots persists Maple collateral position snapshots atomically.
	// Requires loan_id to be set on each snapshot (obtained from SaveLoanSnapshots).
	// All records are inserted in a single transaction - if any record fails, all changes are rolled back.
	// Conflict resolution: ON CONFLICT (loan_id, block_number, block_version) DO UPDATE
	SaveCollateralSnapshots(ctx context.Context, snapshots []*entity.MapleCollateral) error
}
