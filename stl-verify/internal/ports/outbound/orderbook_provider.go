package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// OrderbookProvider streams aggregated L2 orderbook state for a set of symbols
// from a single exchange.
//
// Implementations are responsible for the full snapshot+delta synchronisation:
// they combine a REST or WebSocket snapshot with the live WebSocket delta
// stream and emit a fully synchronised book before the first OrderbookUpdate
// for any symbol. Multiple symbols are multiplexed over the minimum number of
// WebSocket connections the exchange allows, and connections are transparently
// re-established with exponential backoff on failure.
type OrderbookProvider interface {
	// Name returns the exchange identifier (e.g. "binance").
	Name() string

	// Watch subscribes to the given symbols and returns a channel of orderbook
	// updates. Each update carries an independent, fully aggregated book for one
	// symbol. The channel is closed once ctx is cancelled and every underlying
	// connection has drained. Watch returns an error only for problems detected
	// synchronously (e.g. no symbols); transient connection failures are handled
	// internally via reconnection rather than surfaced as errors.
	Watch(ctx context.Context, symbols []string) (<-chan entity.OrderbookUpdate, error)
}
