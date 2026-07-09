// Package archiving decorates the shared Multicaller so raw SC calls are
// captured to durable storage without the indexers knowing about it.
package archiving

import "context"

type blockVersionKey struct{}

type blockNumberKey struct{}

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

// WithBlockNumber stores the block number on the context. The hash-pinned
// ExecuteAtHash path takes no blockNumber argument, so live workers stamp the
// number here alongside WithBlockVersion; the archiving decorator recovers it
// to key the archive to the real block instead of block 0.
func WithBlockNumber(ctx context.Context, number int64) context.Context {
	return context.WithValue(ctx, blockNumberKey{}, number)
}

// BlockNumberFromContext returns the stored block number, or (0, false) when
// absent (e.g. the number-pinned Execute path, which passes it as an argument).
func BlockNumberFromContext(ctx context.Context) (int64, bool) {
	v, ok := ctx.Value(blockNumberKey{}).(int64)
	return v, ok
}
