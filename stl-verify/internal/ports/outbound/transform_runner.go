package outbound

import "context"

// TransformRunner drives the transformation layer's incremental materialization.
//
// Each transformed.<table> has a generated transformed._run_<table>() function
// (emitted from the schema_master register) that reads raw rows past its
// watermark, applies the canonical rename/cast/fill, upserts into the
// transformed hypertable, and advances the watermark. The incremental logic
// lives in those database functions; TransformRunner lists the tables and
// invokes their run functions.
type TransformRunner interface {
	// ListSources returns the transformed tables to run, one per row in
	// transformed._watermark (seeded by the migration).
	ListSources(ctx context.Context) ([]string, error)

	// RunTable invokes transformed._run_<source>() and returns the number of
	// rows upserted in this pass.
	RunTable(ctx context.Context, source string) (int64, error)
}
