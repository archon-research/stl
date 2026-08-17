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
type BlockMetaRepository interface {
	// PendingBlocks returns up to limit blocks referenced by the observation tables on chainID but
	// not yet in block_meta, ordered by (block_number, block_version) and strictly greater than the
	// (afterNumber, afterVersion) keyset cursor. A fresh scan passes afterNumber = -1, afterVersion = -1.
	PendingBlocks(ctx context.Context, chainID int64, limit int, afterNumber int64, afterVersion int) ([]BlockRef, error)

	// Upsert inserts rows into block_meta, ignoring any whose (chain_id, block_number, block_version)
	// is already present (a block's timestamp is immutable once known). Returns rows actually inserted.
	Upsert(ctx context.Context, rows []BlockMetaRow) (int64, error)
}
