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
	// KnownOperationIDs returns every operation_id already stored for the
	// prime. The sync fetches the full operation list from Anchorage on
	// every run and uses this set to insert only what is new.
	KnownOperationIDs(ctx context.Context, primeID int64) (map[string]struct{}, error)
}
