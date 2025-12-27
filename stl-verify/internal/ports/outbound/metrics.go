// Package outbound defines the outbound port interfaces.
package outbound

import "context"

// MetricsRecorder provides an interface for recording application metrics.
// This allows the application layer to record metrics without depending on
// specific telemetry implementations.
type MetricsRecorder interface {
	// RecordReorg records a chain reorganization event.
	// depth is how many blocks were reorganized, fromBlock is the common ancestor,
	// and toBlock is the new chain head after the reorg.
	RecordReorg(ctx context.Context, depth int, fromBlock, toBlock int64)
}
