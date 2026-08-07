package outbound

import "context"

// StatementStatsReader reads per-statement execution counters from the
// application database's own statistics collector.
//
// The counters are CUMULATIVE since the collector was last reset, not per-tick
// deltas, and they are keyed by the server's statement fingerprint. Two
// consequences drive every consumer of this port:
//
//   - Turning successive readings into per-window deltas is the caller's job, and
//     a reading is meaningful only relative to an earlier one. Publishing a reading
//     as if it were a delta re-publishes the whole of the database's history.
//   - A reading may be LOWER than the one before it. The collector can be reset,
//     explicitly or by a server restart, and an individual entry can be evicted
//     under the extension's entry limit and later re-created from zero. Subtracting
//     blindly produces negative values.
//
// This is the canonical statement of those properties; the implementation and its
// consumers point here rather than restating them.
type StatementStatsReader interface {
	// InsertStatements returns one row per INSERT statement fingerprint currently
	// tracked for the connected database. An entry the collector evicts simply
	// stops being returned.
	InsertStatements(ctx context.Context) ([]StatementStat, error)
}

// StatementStat is one statement fingerprint's counters. See StatementStatsReader
// for what cumulative means here and why a value may go down.
type StatementStat struct {
	// QueryID is the server's fingerprint, and the delta-tracking key: stable while
	// the entry lives, shared by every execution of the same statement shape.
	QueryID int64
	// Query is the normalized statement text, with constants replaced by $n.
	Query string
	Calls int64
	// TotalExecTimeSeconds is seconds. Postgres reports milliseconds; the adapter
	// converts so the port carries one unit.
	TotalExecTimeSeconds float64
	Rows                 int64
}
