package uniswapv4bootstrap

import (
	"context"
	"slices"
)

// Progress is how far a run got, together with what that fact is true FOR: the
// pinned block every persisted row was read at. A later attempt of the same run
// resumes from a record only when it was written for this chain and the pinned
// height still names the recorded hash (see Service.resumePoint).
type Progress struct {
	ChainID     int64  `json:"chain_id"`
	PinnedBlock int64  `json:"pinned_block"`
	PinnedHash  string `json:"pinned_hash"`
	// PoolsDone lists the registry row ids of every pool whose keys were all read
	// and persisted at the pin. Whole pools only: a pool is listed once its last
	// batch has committed, so a resumed attempt redoes at most one pool, and
	// redoing one is a no-op write because the batches it re-reads are already
	// stored.
	PoolsDone []int64 `json:"pools_done"`
}

// ProgressStore persists a run's progress somewhere that outlives the process,
// so a run killed mid-snapshot (a deploy rolls this Deployment like any other)
// resumes on the same pin instead of stitching one snapshot across two heights.
//
// LoadProgress reports absence as (zero, false, nil); an error means the record
// could not be read, which is not the same thing and must not be read as "start
// from the beginning".
type ProgressStore interface {
	SaveProgress(ctx context.Context, progress Progress) error
	LoadProgress(ctx context.Context) (Progress, bool, error)
}

func (p Progress) poolDone(poolID int64) bool {
	return slices.Contains(p.PoolsDone, poolID)
}
