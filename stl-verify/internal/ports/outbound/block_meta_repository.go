package outbound

import (
	"context"
	"time"
)

// BlockRef identifies one archived block on a chain: its number and the version (re-org
// generation) of the raw-block object.
type BlockRef struct {
	Number  int64
	Version int
}

// BlockMetaRow is one block_meta dimension row: a block's authoritative on-chain timestamp.
type BlockMetaRow struct {
	ChainID        int64
	BlockNumber    int64
	BlockVersion   int
	BlockTimestamp time.Time
}

// BlockMetaRepository reads the blocks still missing from the block_meta dimension for a chain and
// upserts resolved (block -> timestamp) rows into it.
// BlockWorkList is one run's enumeration of the blocks a chain references but block_meta lacks. The
// referenced set is computed ONCE when the list is opened, so paging through it is an indexed read
// rather than a re-run of the six-table union per batch.
type BlockWorkList interface {
	// Next returns up to limit blocks, ascending, continuing where the previous call stopped.
	Next(ctx context.Context, limit int) ([]BlockRef, error)
	// Close releases the run's resources. Safe to call more than once.
	Close(ctx context.Context)
}

type BlockMetaRepository interface {
	// OpenWorkList materializes, once, the set of blocks chainID references that block_meta lacks,
	// and returns a cursor over it.
	OpenWorkList(ctx context.Context, chainID int64) (BlockWorkList, error)
	// Upsert appends the batch, skipping blocks already present.
	Upsert(ctx context.Context, rows []BlockMetaRow) (int64, error)
}
