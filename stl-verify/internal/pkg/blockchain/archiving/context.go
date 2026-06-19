// Package archiving decorates the shared Multicaller so raw SC calls are
// captured to durable storage without the indexers knowing about it.
package archiving

import "context"

type blockVersionKey struct{}

// WithBlockVersion stores the block (reorg) version on the context. Workers
// call this once per block-event handler entry so archived records carry the
// version the indexer actually processed.
func WithBlockVersion(ctx context.Context, version int) context.Context {
	return context.WithValue(ctx, blockVersionKey{}, version)
}

// BlockVersionFromContext returns the stored block version, or (0, false) when
// absent (correct default for backfills).
func BlockVersionFromContext(ctx context.Context) (int, bool) {
	v, ok := ctx.Value(blockVersionKey{}).(int)
	return v, ok
}
