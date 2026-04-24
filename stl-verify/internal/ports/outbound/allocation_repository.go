package outbound

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// AllocationRepository defines the interface for allocation position persistence.
type AllocationRepository interface {
	SavePositions(ctx context.Context, positions []*entity.AllocationPosition) error
	// SavePositionsTx persists positions within an externally managed transaction,
	// allowing callers to coordinate the write with other repositories in one atomic unit.
	SavePositionsTx(ctx context.Context, tx pgx.Tx, positions []*entity.AllocationPosition) error
}
