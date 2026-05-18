package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// CEXFeedSubscriber yields raw WebSocket frames from a single exchange. The
// implementation owns connection lifecycle (dial, reconnect, ping). Payloads
// are forwarded unparsed; consumers downstream do source-specific decoding.
type CEXFeedSubscriber interface {
	// Subscribe starts the underlying connection and returns a channel of
	// raw frames. The channel is closed when Close is called.
	Subscribe(ctx context.Context) (<-chan entity.RawCEXMessage, error)

	// Close stops the subscription, closes the channel, and releases
	// connection resources.
	Close() error

	// HealthCheck reports whether the subscription is healthy.
	HealthCheck() error
}
