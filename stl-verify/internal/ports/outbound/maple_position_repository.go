package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// MaplePositionRepository defines the interface for Maple Finance position data persistence.
// Maple positions use asset symbols instead of token IDs, so they are stored in
// separate tables from the generic borrower/borrower_collateral tables.
type MaplePositionRepository interface {
	// SaveBorrowerSnapshots persists Maple borrower (debt) position snapshots atomically.
	// All records are inserted in a single transaction - if any record fails, all changes are rolled back.
	// Conflict resolution: ON CONFLICT (user_id, protocol_id, pool_asset, block_number, block_version) DO UPDATE
	SaveBorrowerSnapshots(ctx context.Context, snapshots []*entity.MapleBorrower) error

	// SaveCollateralSnapshots persists Maple collateral position snapshots atomically.
	// All records are inserted in a single transaction - if any record fails, all changes are rolled back.
	// Conflict resolution: ON CONFLICT (user_id, protocol_id, collateral_asset, block_number, block_version) DO UPDATE
	SaveCollateralSnapshots(ctx context.Context, snapshots []*entity.MapleCollateral) error
}
