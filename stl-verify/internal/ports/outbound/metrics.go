// Package outbound defines the outbound port interfaces.
package outbound

import (
	"context"
	"time"
)

// ReorgRecorder records chain reorganization events.
// Used by services that track blockchain state (e.g., live_data).
type ReorgRecorder interface {
	// RecordReorg records a chain reorganization event.
	// depth is how many blocks were reorganized, fromBlock is the common ancestor,
	// and toBlock is the new chain head after the reorg.
	RecordReorg(ctx context.Context, depth int, fromBlock, toBlock int64)
}

// BackupMetricsRecorder records metrics for backup processing.
// Used by services that process messages from queues (e.g., raw_data_backup).
type BackupMetricsRecorder interface {
	// RecordProcessingLatency records the duration of message processing.
	RecordProcessingLatency(ctx context.Context, duration time.Duration, status string)

	// RecordBlockProcessed increments the blocks processed counter.
	RecordBlockProcessed(ctx context.Context, status string)
}
