package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// OrderbookSnapshotRepository persists L2 order book snapshots. The data is
// append-only: each call inserts new rows and never updates or deletes existing
// ones, so a snapshot stream is an immutable time series.
type OrderbookSnapshotRepository interface {
	// Save inserts every snapshot as its own row in one batch. It is a no-op for
	// an empty slice.
	Save(ctx context.Context, snapshots []entity.OrderbookSnapshot) error
}
