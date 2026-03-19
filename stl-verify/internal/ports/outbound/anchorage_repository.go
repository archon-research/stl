package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// AnchorageSnapshotRepository defines the interface for persisting Anchorage
// collateral package snapshots.
type AnchorageSnapshotRepository interface {
	SaveSnapshots(ctx context.Context, snapshots []entity.AnchoragePackageSnapshot) error
}

// AnchorageOperationRepository defines the interface for persisting Anchorage
// collateral management operations (deposits, paydowns, margin returns, etc.).
type AnchorageOperationRepository interface {
	SaveOperations(ctx context.Context, operations []entity.AnchorageOperation) error
	GetLastCursor(ctx context.Context, primeID int64) (string, error)
	GetPrimeIDByName(ctx context.Context, name string) (int64, error)
}
