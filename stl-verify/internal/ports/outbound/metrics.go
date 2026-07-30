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

	// RecordOutOfOrderBlock records that a block arrived with a number at or
	// below the current canonical head (out-of-order / late delivery from
	// upstream). The outcome label (OutOfOrderOutcome*) says how it was
	// classified. This is the direct diagnostic for the VEC-277 trigger:
	// a sustained nonzero rate means upstream is delivering headers out of
	// order, which under load misclassifies as reorgs and over-orphans.
	RecordOutOfOrderBlock(ctx context.Context, outcome string)
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

// Outcome labels for RecordOutOfOrderBlock. An out-of-order block is one whose
// number is at or below the current canonical head (it arrived after a higher
// block). The outcome distinguishes the benign late-arrival fill from a genuine
// reorg. A spike in either is the direct signal of out-of-order delivery from
// upstream (the VEC-277 trigger).
const (
	// OutOfOrderOutcomeLateArrival: the low block links cleanly onto the
	// canonical chain and no competing block holds its height, so it is saved
	// as a gap fill rather than treated as a reorg.
	OutOfOrderOutcomeLateArrival = "late_arrival"

	// OutOfOrderOutcomeReorg: the low block conflicts (a different canonical
	// block holds its height, or it does not link onto our chain), so it is
	// routed through reorg handling.
	OutOfOrderOutcomeReorg = "reorg"
)

// BackfillRecorder records observability events from the backfill gap-fill
// loop. The single hook today is RecordBackfillGapNoCanonical, fired by the
// post-cycle invariant check that catches "gap-fill returned success but no
// canonical row exists" (the silent-failure mode behind VEC-277 arbitrum
// backfill).
type BackfillRecorder interface {
	// RecordBackfillGapNoCanonical increments the counter that fires when a
	// per-block gap-fill cycle completes without producing a non-orphaned
	// canonical row. Labelled by chain.
	RecordBackfillGapNoCanonical(ctx context.Context, chainID int64)

	// RecordWatermarkLag records the current backfill lag (highest known block
	// minus the backfill watermark) as a gauge. Sustained or growing lag is the
	// direct symptom that surfaced 26 days late in the VEC-277 incident: the
	// watermark stayed pinned while head advanced.
	RecordWatermarkLag(ctx context.Context, lag int64)
}

// BackupMetricsRecorder records metrics for backup processing.
// Used by services that process messages from queues (e.g., raw_data_backup).
type BackupMetricsRecorder interface {
	// RecordProcessingLatency records the duration of message processing.
	RecordProcessingLatency(ctx context.Context, duration time.Duration, status string)

	// RecordBlockProcessed increments the blocks processed counter.
	RecordBlockProcessed(ctx context.Context, status string)
}

// Canonical `status` label values for the two BackupMetricsRecorder methods,
// shared so every emitter and the alerts that key on them (e.g. Vector's
// blocks_processed_total{status="error"} rules) stay in lockstep instead of each
// service inlining its own literal. StatusSuccess = the unit of work completed
// without error (any due snapshots were persisted); StatusError = it failed and
// will be retried. Services with a richer outcome vocabulary (e.g.
// raw_data_backup's already_backed_up / rpc_fallback) define those locally.
const (
	StatusSuccess = "success"
	StatusError   = "error"
)
