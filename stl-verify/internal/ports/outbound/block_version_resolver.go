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
}
