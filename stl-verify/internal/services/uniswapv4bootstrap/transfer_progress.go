package uniswapv4bootstrap

import "context"

// TransferProgress is how far a posm transfer backfill got, together with what
// that fact is true FOR: the chain, and the pinned height the run scans up to. A
// later attempt of the same run resumes from a record only when it was written
// for this chain and this PositionManager, and the pinned height still names the
// recorded hash (see TransferService.resumePoint).
type TransferProgress struct {
	ChainID int64 `json:"chain_id"`
	// PositionManagerID is the registry row every row written so far FKs. It is
	// part of the scope because the posm is re-read from the database at every
	// worker boot: a correcting registry version landing between attempts gives
	// the chain a new surrogate id, and a cursor carried over to it would skip
	// every transfer below itself under that id, which no rerun looks at again.
	PositionManagerID int64  `json:"position_manager_id"`
	PinnedBlock       int64  `json:"pinned_block"`
	PinnedHash        string `json:"pinned_hash"`
	// NextBlock is the lowest height not yet persisted: one past the last scan
	// window whose rows all committed. Whole windows only, so a resumed attempt
	// redoes at most one window, and redoing one writes nothing because every
	// site it revisits already holds a row.
	NextBlock int64 `json:"next_block"`
}

// TransferProgressStore persists a transfer backfill's progress somewhere that
// outlives the process, so a run killed mid-scan resumes near where it stopped
// instead of replaying four million blocks.
//
// LoadProgress reports absence as (zero, false, nil); an error means the record
// could not be read, which is not the same thing and must not be read as "start
// from the beginning".
type TransferProgressStore interface {
	SaveProgress(ctx context.Context, progress TransferProgress) error
	LoadProgress(ctx context.Context) (TransferProgress, bool, error)
}

// scopeMatches reports whether this record was written for the same chain and
// PositionManager the caller is about to resume under.
func (p TransferProgress) scopeMatches(chainID, positionManagerID int64) bool {
	return p.ChainID == chainID && p.PositionManagerID == positionManagerID
}
