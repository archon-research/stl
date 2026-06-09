package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// OrderbookProvider streams aggregated L2 order book state for a set of symbols
// from one exchange. Implementations own the full snapshot+delta sync and
// reconnect internally with backoff.
type OrderbookProvider interface {
	// Name returns the exchange identifier (e.g. "okx").
	Name() string

	// Watch streams an independent, fully aggregated book per symbol until ctx is
	// cancelled (then the channel closes). It errors only on synchronous problems
	// (e.g. no symbols, or a malformed one); connection failures are handled by
	// reconnection. A symbol the venue rejects at subscribe time (e.g. an unknown
	// pair) is NOT surfaced as an error: the provider logs it at error level and
	// that symbol simply never produces updates.
	Watch(ctx context.Context, symbols []string) (<-chan entity.OrderbookUpdate, error)
}
