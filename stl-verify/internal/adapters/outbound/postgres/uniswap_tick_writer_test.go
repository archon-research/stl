package postgres

import (
	"math"
	"math/big"
	"testing"
)

// TestDistinctSortedUniswapTickKeys pins the advisory-lock ordering: every
// SaveBlock must request overlapping (pool_id, tick) slots in the same
// sequence, or two concurrent block writers deadlock against each other.
func TestDistinctSortedUniswapTickKeys(t *testing.T) {
	tick := func(poolID int64, tick int) uniswapTickRow {
		return uniswapTickRow{poolID: poolID, tick: tick}
	}

	got := distinctSortedUniswapTickKeys([]uniswapTickRow{
		tick(7, 120),
		tick(2, 60),
		tick(7, -240),
		tick(2, 60),
		tick(2, -60),
	})

	want := []uniswapTickKey{
		{poolID: 2, tick: -60},
		{poolID: 2, tick: 60},
		{poolID: 7, tick: -240},
		{poolID: 7, tick: 120},
	}
	if len(got) != len(want) {
		t.Fatalf("keys = %v, want %v (duplicates must collapse)", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("key %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestSharedBlockNumberRejectsMixedBlocks guards the height bound the
// latest-row read applies: a batch spanning blocks would compare a tick against
// a row from a different block and silently skip the insert.
func TestSharedBlockNumberRejectsMixedBlocks(t *testing.T) {
	rows := []uniswapTickRow{
		{poolID: 1, tick: 60, blockNumber: 100, liquidityGross: big.NewInt(1)},
		{poolID: 1, tick: 120, blockNumber: 101, liquidityGross: big.NewInt(1)},
	}
	if _, err := uniswapV4TickWriter.sharedBlockNumber(rows); err == nil {
		t.Fatal("sharedBlockNumber across two blocks: want error, got nil")
	}

	rows[1].blockNumber = 100
	got, err := uniswapV4TickWriter.sharedBlockNumber(rows)
	if err != nil {
		t.Fatalf("sharedBlockNumber: %v", err)
	}
	if got != 100 {
		t.Errorf("blockNumber = %d, want 100", got)
	}
}

// TestUniswapTickWriterUnchanged pins the append-on-change decision, whose two
// halves are load-bearing for correctness: block_version separates a reorg
// re-observation from a redelivery, but only within one height — across heights
// the versions belong to different blocks and only the values may decide.
func TestUniswapTickWriterUnchanged(t *testing.T) {
	stored := func(blockNumber int64, blockVersion int, liquidityNet int64) uniswapTickValues {
		return uniswapTickValues{
			blockNumber:           blockNumber,
			blockVersion:          blockVersion,
			liquidityGross:        big.NewInt(1000),
			liquidityNet:          big.NewInt(liquidityNet),
			feeGrowthOutside0X128: big.NewInt(2),
			feeGrowthOutside1X128: big.NewInt(3),
		}
	}
	candidate := func(blockNumber int64, blockVersion int, liquidityNet int64) uniswapTickRow {
		return uniswapTickRow{
			poolID:                1,
			tick:                  60,
			blockNumber:           blockNumber,
			blockVersion:          blockVersion,
			liquidityGross:        big.NewInt(1000),
			liquidityNet:          big.NewInt(liquidityNet),
			feeGrowthOutside0X128: big.NewInt(2),
			feeGrowthOutside1X128: big.NewInt(3),
		}
	}

	for _, tc := range []struct {
		name  string
		prior uniswapTickValues
		next  uniswapTickRow
		want  bool
	}{
		{"same_block_same_version_same_values", stored(100, 0, 1), candidate(100, 0, 1), true},
		{"same_block_reorg_version", stored(100, 0, 1), candidate(100, 1, 1), false},
		{"later_block_same_values", stored(100, 0, 1), candidate(101, 0, 1), true},
		{"later_block_after_reorg_same_values", stored(100, 1, 1), candidate(101, 0, 1), true},
		{"later_block_changed_value", stored(100, 0, 1), candidate(101, 0, 2), false},
		{"earlier_stored_block_changed_value", stored(99, 0, 1), candidate(100, 0, 2), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := uniswapV4TickWriter.unchanged(tc.prior, tc.next); got != tc.want {
				t.Errorf("unchanged = %t, want %t", got, tc.want)
			}
		})
	}
}

// TestUniswapTickWriterUnchangedComparesInitialized covers the one column the
// two tables disagree on: V3 must append when initialized flips, and V4, whose
// table has no such column, must never read anything into that decision.
func TestUniswapTickWriterUnchangedComparesInitialized(t *testing.T) {
	prior := uniswapTickValues{
		blockNumber: 100, blockVersion: 0, initialized: true,
		liquidityGross: big.NewInt(1), liquidityNet: big.NewInt(1),
		feeGrowthOutside0X128: big.NewInt(1), feeGrowthOutside1X128: big.NewInt(1),
	}
	next := uniswapTickRow{
		poolID: 1, tick: 60, blockNumber: 101, blockVersion: 0, initialized: false,
		liquidityGross: big.NewInt(1), liquidityNet: big.NewInt(1),
		feeGrowthOutside0X128: big.NewInt(1), feeGrowthOutside1X128: big.NewInt(1),
	}

	if uniswapV3TickWriter.unchanged(prior, next) {
		t.Error("v3 unchanged = true, want false (a flipped initialized must append)")
	}
	if !uniswapV4TickWriter.unchanged(prior, next) {
		t.Error("v4 unchanged = false, want true (the v4 table has no initialized column)")
	}
}

// TestDistinctSortedUniswapTickKeysOrdersExtremePoolIDs documents the
// comparator's contract, not a reachable scenario: pool_id is a BIGSERIAL, so
// the database cannot produce a negative or near-MaxInt64 id. The lock order is
// a deadlock invariant, so the comparator must be correct by construction
// rather than by the range of its inputs.
func TestDistinctSortedUniswapTickKeysOrdersExtremePoolIDs(t *testing.T) {
	tick := func(poolID int64, tick int) uniswapTickRow {
		return uniswapTickRow{poolID: poolID, tick: tick}
	}

	got := distinctSortedUniswapTickKeys([]uniswapTickRow{
		tick(math.MaxInt64, 60),
		tick(math.MinInt64, 60),
	})

	want := []uniswapTickKey{
		{poolID: math.MinInt64, tick: 60},
		{poolID: math.MaxInt64, tick: 60},
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("key %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}
