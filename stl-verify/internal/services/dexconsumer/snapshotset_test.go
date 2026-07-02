package dexconsumer

import (
	"testing"
)

// testPool is a minimal SnapshotPool used to test SnapshotTracker without
// depending on any concrete DEX package (dexconsumer must not import one).
type testPool struct {
	id          int64
	deployBlock int64
}

func (p testPool) PoolID() int64         { return p.id }
func (p testPool) DeployBlockNum() int64 { return p.deployBlock }

func pool(id, deployBlock int64) testPool {
	return testPool{id: id, deployBlock: deployBlock}
}

// TestDueSet_SweepDue covers every sweep-due condition: unseen, interval
// elapsed, and same-block-different-version (reorg).
func TestDueSet_SweepDue(t *testing.T) {
	tests := []struct {
		name        string
		sweepBlocks int64
		mark        *snapKey
		bn          int64
		ver         int
		wantDue     bool
	}{
		{
			name:        "unseen pool is due",
			sweepBlocks: 5,
			mark:        nil,
			bn:          100,
			ver:         0,
			wantDue:     true,
		},
		{
			name:        "interval not yet elapsed",
			sweepBlocks: 5,
			mark:        &snapKey{bn: 100, ver: 0},
			bn:          104,
			ver:         0,
			wantDue:     false,
		},
		{
			name:        "interval elapsed",
			sweepBlocks: 5,
			mark:        &snapKey{bn: 100, ver: 0},
			bn:          105,
			ver:         0,
			wantDue:     true,
		},
		{
			name:        "same block, different version (reorg)",
			sweepBlocks: 5,
			mark:        &snapKey{bn: 100, ver: 0},
			bn:          100,
			ver:         1,
			wantDue:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := NewSnapshotTracker(tt.sweepBlocks)
			p := pool(1, 0)
			if tt.mark != nil {
				tracker.MarkSnapshotted([]int64{p.PoolID()}, tt.mark.bn, tt.mark.ver)
			}

			got, err := DueSet(tracker, []testPool{p}, map[int64]bool{}, tt.bn, tt.ver)
			if err != nil {
				t.Fatalf("DueSet: %v", err)
			}

			isDue := len(got) == 1
			if isDue != tt.wantDue {
				t.Errorf("due = %v, want %v (result=%v)", isDue, tt.wantDue, got)
			}
		})
	}
}

// TestDueSet_SweepDuePoolExcludedBelowDeployBlock: a sweep-due pool whose
// deploy block is ahead of the current block must be excluded, not just
// deferred - it does not exist on-chain yet.
func TestDueSet_SweepDuePoolExcludedBelowDeployBlock(t *testing.T) {
	tracker := NewSnapshotTracker(5)
	notYetDeployed := pool(1, 200) // deploy block 200

	got, err := DueSet(tracker, []testPool{notYetDeployed}, map[int64]bool{}, 100, 0)
	if err != nil {
		t.Fatalf("DueSet: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("result = %v, want empty (pool deploys at 200, block is 100)", got)
	}
}

// TestDueSet_TouchedPoolBelowDeployBlockErrors: a touched pool can never be
// below its own deploy block on real chain data (a contract cannot emit logs
// before it is deployed), so this is a registry bug - fail loud.
func TestDueSet_TouchedPoolBelowDeployBlockErrors(t *testing.T) {
	tracker := NewSnapshotTracker(0)
	impossiblyTouched := pool(1, 200) // deploy block 200

	_, err := DueSet(tracker, []testPool{impossiblyTouched}, map[int64]bool{1: true}, 100, 0)
	if err == nil {
		t.Fatal("expected error for touched pool below its deploy block, got nil")
	}
}

// TestDueSet_TouchedIDMissingFromRegistryErrors: a touched pool ID with no
// corresponding entry in the registry (all) is a registry/lookup bug - the
// caller derived the touched set from pools it claims to know about, so a
// mismatch here must fail loud rather than be silently ignored.
func TestDueSet_TouchedIDMissingFromRegistryErrors(t *testing.T) {
	tracker := NewSnapshotTracker(0)
	registered := pool(1, 0)

	_, err := DueSet(tracker, []testPool{registered}, map[int64]bool{1: true, 999: true}, 100, 0)
	if err == nil {
		t.Fatal("expected error for touched pool ID absent from the registry, got nil")
	}
}

// TestDueSet_TouchedAndSweepDueDeduped: a pool that is both touched and
// sweep-due must appear exactly once in the result.
func TestDueSet_TouchedAndSweepDueDeduped(t *testing.T) {
	tracker := NewSnapshotTracker(5)
	p := pool(7, 0)

	got, err := DueSet(tracker, []testPool{p}, map[int64]bool{7: true}, 100, 0)
	if err != nil {
		t.Fatalf("DueSet: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("result = %v, want exactly one entry for pool touched and sweep-due", got)
	}
}

// TestDueSet_SortedByPoolIDAscending: the DB trigger's advisory-lock
// convention requires deterministic pool-ID-ascending order.
func TestDueSet_SortedByPoolIDAscending(t *testing.T) {
	tracker := NewSnapshotTracker(0)
	all := []testPool{pool(30, 0), pool(10, 0), pool(20, 0)}
	touched := map[int64]bool{30: true, 10: true, 20: true}

	got, err := DueSet(tracker, all, touched, 100, 0)
	if err != nil {
		t.Fatalf("DueSet: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("result = %v, want 3 entries", got)
	}
	for i := 1; i < len(got); i++ {
		if got[i-1].PoolID() >= got[i].PoolID() {
			t.Errorf("result not sorted ascending by PoolID: %v", got)
		}
	}
}

// TestDueSet_SweepDisabledOnlyTouched: sweepBlocks == 0 disables sweep
// entirely; only touched pools are returned, still deploy-gated.
func TestDueSet_SweepDisabledOnlyTouched(t *testing.T) {
	tracker := NewSnapshotTracker(0)
	touchedPool := pool(1, 0)
	untouchedPool := pool(2, 0)

	got, err := DueSet(tracker, []testPool{touchedPool, untouchedPool}, map[int64]bool{1: true}, 100, 0)
	if err != nil {
		t.Fatalf("DueSet: %v", err)
	}
	if len(got) != 1 || got[0].PoolID() != 1 {
		t.Errorf("result = %v, want only pool 1 (sweep disabled)", got)
	}
}

// TestDueSet_MarkSnapshottedThenDueSetReflectsRecordedVersion: after
// MarkSnapshotted records (bn, ver), a subsequent DueSet call at the same
// (bn, ver) must NOT treat the pool as sweep-due (it was just snapshotted),
// but a different version at the same block (reorg) must.
func TestDueSet_MarkSnapshottedThenDueSetReflectsRecordedVersion(t *testing.T) {
	tracker := NewSnapshotTracker(5)
	p := pool(1, 0)

	tracker.MarkSnapshotted([]int64{p.PoolID()}, 100, 0)

	gotSameVer, err := DueSet(tracker, []testPool{p}, map[int64]bool{}, 100, 0)
	if err != nil {
		t.Fatalf("DueSet (same version): %v", err)
	}
	if len(gotSameVer) != 0 {
		t.Errorf("result = %v, want empty (just snapshotted at same bn/ver)", gotSameVer)
	}

	gotNewVer, err := DueSet(tracker, []testPool{p}, map[int64]bool{}, 100, 1)
	if err != nil {
		t.Fatalf("DueSet (new version): %v", err)
	}
	if len(gotNewVer) != 1 {
		t.Errorf("result = %v, want pool due (reorg: same block, new version)", gotNewVer)
	}
}
