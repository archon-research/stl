package archiving

import "context"

// blockVersionKey is unexported so callers must use WithBlockVersion /
// BlockVersionFromContext rather than poking the context directly.
type blockVersionKey struct{}

// WithBlockVersion attaches the canonical block_version of the currently
// processed block to ctx. The archiving Multicaller reads this when assembling
// the S3 key so the archive's `bv=` segment matches the actual chain reorg
// version — VEC-81 partitioning relies on this for audit correctness on
// reorged blocks. Services should call this once per block handler entry:
//
//	ctx = archiving.WithBlockVersion(ctx, event.Version)
//
// When the value is not present the archiver defaults to bv=0, which is
// correct for backfills (canonical, never reorged) but silently wrong for
// live workers — hence the explicit context plumbing.
func WithBlockVersion(ctx context.Context, version int) context.Context {
	return context.WithValue(ctx, blockVersionKey{}, version)
}

// BlockVersionFromContext returns the block_version attached by
// WithBlockVersion. ok is false when none was attached.
func BlockVersionFromContext(ctx context.Context) (version int, ok bool) {
	v, ok := ctx.Value(blockVersionKey{}).(int)
	return v, ok
}
