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

// S3BackupRecorder records metrics for the watcher's inline S3 backup path.
// The watcher kicks off parallel S3 PUTs as soon as block data is in memory and
// awaits them before publishing to SNS, so its critical-path budget includes the
// slowest of the per-data-type PUTs. These metrics let operators see both
// individual PUT latency and the wall-clock cost of the parallel group.
type S3BackupRecorder interface {
	// RecordPutDuration records the duration of a single S3 PUT for a data type.
	// outcome ∈ {"success", "skipped", "error"}.
	RecordPutDuration(ctx context.Context, dataType, outcome string, duration time.Duration)

	// RecordPutBytes records the size of a written object in bytes.
	RecordPutBytes(ctx context.Context, dataType string, bytes int64)

	// RecordPutError counts a terminal PUT failure with a bounded error_class
	// label so dashboards can distinguish throttling from auth/5xx/timeout.
	RecordPutError(ctx context.Context, dataType, errorClass string)

	// RecordGroupDuration records the wall-clock duration of the parallel S3
	// PUT group (errgroup.Wait). outcome ∈ {"success", "error"}.
	RecordGroupDuration(ctx context.Context, outcome string, duration time.Duration)
}

// S3BackupErrorClass labels are the bounded set used by RecordPutError.
// Keep cardinality fixed — never pass a raw error string.
const (
	S3BackupErrorClassThrottle = "throttle"
	S3BackupErrorClass5xx      = "5xx"
	S3BackupErrorClass4xx      = "4xx"
	S3BackupErrorClassTimeout  = "timeout"
	S3BackupErrorClassAuth     = "auth"
	S3BackupErrorClassOther    = "other"
)
