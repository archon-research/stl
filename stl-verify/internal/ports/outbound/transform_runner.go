package outbound

import "context"

// TransformRunner drives the transformation layer's incremental materialization.
//
// Each transformed.<table> has a generated transformed._run_<table>() function
// (emitted from the schema_master register) that drains that table's change
// queue (transformed._pending_<table>, populated by an AFTER INSERT trigger on
// the raw table), re-reads just the queued raw rows by primary key, applies the
// canonical rename/cast/fill, and upserts into the transformed hypertable
// (ON CONFLICT DO UPDATE guarded by IS DISTINCT FROM, so a re-run refreshes a
// changed row but skips the write when nothing changed). The incremental logic
// lives in those database functions; TransformRunner lists the tables and
// invokes their run functions.
type TransformRunner interface {
	// ListSources returns the transformed tables to run, one per row in
	// transformed._sources (seeded by the migration).
	ListSources(ctx context.Context) ([]string, error)

	// RunTable drains a source's queue to empty and returns the number of queue
	// rows consumed in this pass.
	RunTable(ctx context.Context, source string) (int64, error)

	// QueueStatus returns the per-source change-queue backlog, for the
	// stalled-transform signal (a source whose oldest enqueue keeps ageing is
	// not draining).
	QueueStatus(ctx context.Context) ([]QueueDepth, error)

	// ParityStatus returns the per-source raw-vs-transformed parity, the backstop
	// that catches a silent queue-mechanism failure (a trigger gap, a bootstrap
	// miss, or an append-only violation) that QueueStatus cannot see.
	ParityStatus(ctx context.Context) ([]ParityRow, error)
}

// ParityRow is one source's raw-vs-transformed row-count parity at a point in
// time. In a consistent snapshot after bootstrap every raw row is either in the
// transformed table or still in its pending queue, so Drift (= RawRows -
// TransformedRows - PendingRows) is 0; any nonzero value is a real divergence.
type ParityRow struct {
	Source          string
	RawRows         int64
	TransformedRows int64
	PendingRows     int64
	Drift           int64
}

// QueueDepth is one source's change-queue backlog at a point in time.
type QueueDepth struct {
	Source string
	// Pending is the number of un-drained rows in transformed._pending_<source>.
	Pending int64
	// OldestAgeSecs is seconds since the oldest un-drained row was enqueued; 0
	// when the queue is empty.
	OldestAgeSecs float64
}
