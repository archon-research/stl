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

	// RecordReorgDropped records a reorg signal that was rejected before any
	// state mutation. The reason argument identifies which gate dropped it
	// (use the ReorgDropReason* constants below). Operators monitor this to
	// track stale-fork broadcasts, RPC errors during verification, and TOCTOU
	// races between live ingestion and concurrent writers (VEC-202).
	RecordReorgDropped(ctx context.Context, reason string)
}

// Reason constants for ReorgRecorder.RecordReorgDropped. Stable label values
// for the resulting metric so dashboards and alerts can rely on them.
const (
	// ReorgDropReasonStaleFork: RPC's canonical block at the incoming
	// number does not match the incoming hash. The broadcast is on a fork
	// that did not win.
	ReorgDropReasonStaleFork = "stale_fork"

	// ReorgDropReasonVerifyError: RPC verification call failed. Conservative
	// drop — the next live broadcast will retry once RPC stabilises.
	ReorgDropReasonVerifyError = "verify_error"

	// ReorgDropReasonStateShifted: another writer committed to block_states
	// during the verify round-trip, so the precomputed reorgEvent no longer
	// matches the current DB state. The next live broadcast will trigger a
	// fresh detect→verify→commit pass.
	ReorgDropReasonStateShifted = "state_shifted"
)

// BackupMetricsRecorder records metrics for backup processing.
// Used by services that process messages from queues (e.g., raw_data_backup).
type BackupMetricsRecorder interface {
	// RecordProcessingLatency records the duration of message processing.
	RecordProcessingLatency(ctx context.Context, duration time.Duration, status string)

	// RecordBlockProcessed increments the blocks processed counter.
	RecordBlockProcessed(ctx context.Context, status string)
}
