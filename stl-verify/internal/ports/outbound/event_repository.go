package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/jackc/pgx/v5"
)

// EventRepository defines the interface for protocol event persistence.
type EventRepository interface {
	// SaveEvent saves a single protocol event within an external transaction.
	// Uses ON CONFLICT DO NOTHING — duplicate events are silently ignored.
	SaveEvent(ctx context.Context, tx pgx.Tx, event *entity.ProtocolEvent) error
}
