package outbound

import (
	"context"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// OrderbookRepository defines the interface for orderbook data persistence.
type OrderbookRepository interface {
	// SaveSnapshots stores one or more orderbook snapshots.
	SaveSnapshots(ctx context.Context, snapshots []*entity.OrderbookSnapshot) error

	// GetLatestSnapshot returns the most recent snapshot for a given exchange and symbol.
	GetLatestSnapshot(ctx context.Context, exchange, symbol string) (*entity.OrderbookSnapshot, error)

	// GetLatestSnapshotsForSymbol returns the most recent snapshot from each exchange for a symbol.
	GetLatestSnapshotsForSymbol(ctx context.Context, symbol string) ([]*entity.OrderbookSnapshot, error)

	// GetSnapshotsInRange returns snapshots for a symbol within a time range.
	GetSnapshotsInRange(ctx context.Context, symbol string, from, to time.Time) ([]*entity.OrderbookSnapshot, error)
}
