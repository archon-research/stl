package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// CEXFeedPublisher publishes raw WebSocket frames downstream (typically to
// SNS) so that horizontally-scaled indexer workers can consume and parse
// them via SQS.
type CEXFeedPublisher interface {
	// Publish sends a single raw message. Implementations should be safe
	// for concurrent use.
	Publish(ctx context.Context, msg entity.RawCEXMessage) error

	// Close releases any underlying client resources.
	Close() error
}
