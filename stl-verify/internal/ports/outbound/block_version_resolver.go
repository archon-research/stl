package outbound

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
)

// BlockVersionResolver answers which block_version a height was indexed under, for a
// replay that reads its events from a node rather than from the archive and so carries
// no version of its own. Stamping a constant instead puts the replayed row beside the
// live one at a version the ordering tuple ranks against it, rather than deduping with
// it — VEC-218.
type BlockVersionResolver interface {
	// ResolveBlockVersion returns the version the raw archive holds for the block
	// (blockNumber, blockHash). It fails when the archive holds nothing at that height,
	// and when what it holds names another block: a replay that cannot prove which
	// version speaks for a block must stop rather than stamp a guess.
	ResolveBlockVersion(ctx context.Context, blockNumber int64, blockHash common.Hash) (int, error)

	// Summary reports what has been resolved so far, for the one line a run logs at the
	// end of it. Without it a replay that stamped corrected versions is indistinguishable
	// from one that stamped 0 everywhere.
	Summary() ResolvedVersions
}

// ResolvedVersions is what a run asked the archive for: how many heights it answered,
// and which of those it holds under a corrected (non-zero) version — the blocks whose
// replayed rows do not land at version 0.
type ResolvedVersions struct {
	Heights   int
	Corrected []int64
}
