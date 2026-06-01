package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

type ExchangeOrderBookStreamer interface {
	// Name returns the exchange identifier, e.g. "binance".
	Name() string

	// Connect validates symbols and prepares the adapter to stream snapshots.
	// Returns an error if symbols is empty or the adapter cannot initialize.
	Connect(ctx context.Context, symbols []string) error

	// Stream starts emitting normalized snapshots until context cancellation,
	// Close, or a fatal error. The error channel receives at most one terminal
	// error; the snapshot channel is closed when the goroutine exits.
	Stream(ctx context.Context) (<-chan entity.OrderBookSnapshot, <-chan error)

	// Close shuts down the adapter and releases resources.
	Close() error
}
