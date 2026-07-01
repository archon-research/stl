package dexconsumer

import (
	"fmt"
	"sort"
)

// SnapshotPool is the minimal view SnapshotTracker needs of a pool. Any DEX's
// concrete pool type implements this to reuse the shared sweep/deploy-gate
// bookkeeping without dexconsumer depending on that DEX's package.
type SnapshotPool interface {
	PoolID() int64
	DeployBlockNum() int64
}

// snapKey records the (blockNumber, version) of a pool's last persisted
// snapshot, so sweep logic correctly detects reorgs where bn == lastBn but
// version changed.
type snapKey struct {
	bn  int64
	ver int
}

// SnapshotTracker decides which pools to snapshot each block (touched ∪
// sweep-due, gated by deploy block) and remembers the last snapshot per pool
// for sweep timing.
//
// Single-goroutine use only (matches the SQS RunLoop contract); no internal
// locking.
type SnapshotTracker struct {
	sweepBlocks  int64
	lastSnapshot map[int64]snapKey
}

// NewSnapshotTracker builds a tracker. sweepBlocks <= 0 disables periodic
// sweeping; only touched pools will ever be due.
func NewSnapshotTracker(sweepBlocks int64) *SnapshotTracker {
	return &SnapshotTracker{
		sweepBlocks:  sweepBlocks,
		lastSnapshot: make(map[int64]snapKey),
	}
}

// MarkSnapshotted records that these pools were snapshotted at (bn, ver).
func (t *SnapshotTracker) MarkSnapshotted(poolIDs []int64, bn int64, ver int) {
	for _, id := range poolIDs {
		t.lastSnapshot[id] = snapKey{bn: bn, ver: ver}
	}
}

// DueSet returns, sorted by PoolID ascending, the pools to snapshot this
// block: every touched pool plus every sweep-due pool, excluding any pool
// whose DeployBlockNum() > bn. `touched` is the set of pool IDs touched this
// block.
//
// A touched pool below its own deploy block is impossible on real chain data
// (a contract cannot emit logs before it is deployed), so that combination
// indicates a registry bug and is reported as an error rather than silently
// included or dropped.
//
// Generic free function, not a method, because Go methods cannot have type
// parameters; callers get their concrete pool type back with no cast, and
// dexconsumer takes on no dependency on any DEX package.
func DueSet[P SnapshotPool](t *SnapshotTracker, all []P, touched map[int64]bool, bn int64, ver int) ([]P, error) {
	byID := make(map[int64]P, len(touched))
	remainingTouched := make(map[int64]bool, len(touched))
	for id, isTouched := range touched {
		if isTouched {
			remainingTouched[id] = true
		}
	}
	for _, p := range all {
		if !touched[p.PoolID()] {
			continue
		}
		delete(remainingTouched, p.PoolID())
		if p.DeployBlockNum() > bn {
			return nil, fmt.Errorf("pool %d touched at block %d but registry deploy block is %d: registry bug", p.PoolID(), bn, p.DeployBlockNum())
		}
		byID[p.PoolID()] = p
	}
	if len(remainingTouched) > 0 {
		return nil, fmt.Errorf("touched pool IDs %v not found in registry: registry bug", sortedKeys(remainingTouched))
	}

	if t.sweepBlocks > 0 {
		for _, p := range all {
			if p.DeployBlockNum() > bn {
				continue
			}
			last, seen := t.lastSnapshot[p.PoolID()]
			if !seen || bn-last.bn >= t.sweepBlocks || (bn == last.bn && ver != last.ver) {
				byID[p.PoolID()] = p
			}
		}
	}

	result := make([]P, 0, len(byID))
	for _, p := range byID {
		result = append(result, p)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].PoolID() < result[j].PoolID()
	})
	return result, nil
}

// sortedKeys returns a set's keys sorted ascending, for deterministic error
// messages.
func sortedKeys(set map[int64]bool) []int64 {
	keys := make([]int64, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}
