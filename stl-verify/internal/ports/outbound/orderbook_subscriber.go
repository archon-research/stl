package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// OrderbookSubscriber defines the interface for receiving orderbook updates.
// Implementations may use WebSocket streams, REST polling, or any other transport.
type OrderbookSubscriber interface {
	// Subscribe starts receiving orderbook snapshots.
	// The returned channel emits snapshots as they arrive.
	// Call Unsubscribe to stop and release resources.
	Subscribe(ctx context.Context) (<-chan entity.OrderbookSnapshot, error)

	// Unsubscribe stops the subscription and closes the snapshot channel.
	Unsubscribe() error

	// HealthCheck reports whether the subscriber is operational.
	HealthCheck(ctx context.Context) error
}
