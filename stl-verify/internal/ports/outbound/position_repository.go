package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// PositionRepository defines the interface for user position data persistence.
// This aggregate includes borrower (debt) positions and collateral positions.
type PositionRepository interface {
	// UpsertBorrowers upserts borrower (debt) position records.
	// Each record represents a user's debt in a specific token at a specific block.
	// Conflict resolution: ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version) DO UPDATE
	UpsertBorrowers(ctx context.Context, borrowers []*entity.Borrower) error

	// UpsertBorrowerCollateral upserts collateral position records.
	// Each record represents a user's collateral in a specific token at a specific block.
	// Conflict resolution: ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version) DO UPDATE
	UpsertBorrowerCollateral(ctx context.Context, collateral []*entity.BorrowerCollateral) error
}
