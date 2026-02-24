package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// AllocationRepository defines the interface for allocation position persistence.
type AllocationRepository interface {
	SavePositions(ctx context.Context, positions []*entity.AllocationPosition) error
}
