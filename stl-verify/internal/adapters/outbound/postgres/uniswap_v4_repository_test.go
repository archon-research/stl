package postgres

import (
	"math/big"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// TestDistinctSortedV4TickKeys pins the advisory-lock ordering: every
// SaveBlock must request overlapping (pool_id, tick) slots in the same
// sequence, or two concurrent block writers deadlock against each other.
func TestDistinctSortedV4TickKeys(t *testing.T) {
	tick := func(poolID int64, tick int) *entity.UniswapV4Tick {
		return &entity.UniswapV4Tick{PoolID: poolID, Tick: tick}
	}

	got := distinctSortedV4TickKeys([]*entity.UniswapV4Tick{
		tick(7, 120),
		tick(2, 60),
		tick(7, -240),
		tick(2, 60),
		tick(2, -60),
	})

	want := []v4TickKey{
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

// TestSharedBlockNumberRejectsMixedBlocks guards the height bound
// readLatestTicksV4 applies: a batch spanning blocks would compare a tick
// against a row from a different block and silently skip the insert.
func TestSharedBlockNumberRejectsMixedBlocks(t *testing.T) {
	rows := []*entity.UniswapV4Tick{
		{PoolID: 1, Tick: 60, BlockNumber: 100, LiquidityGross: big.NewInt(1)},
		{PoolID: 1, Tick: 120, BlockNumber: 101, LiquidityGross: big.NewInt(1)},
	}
	if _, err := sharedBlockNumber(rows); err == nil {
		t.Fatal("sharedBlockNumber across two blocks: want error, got nil")
	}

	rows[1].BlockNumber = 100
	got, err := sharedBlockNumber(rows)
	if err != nil {
		t.Fatalf("sharedBlockNumber: %v", err)
	}
	if got != 100 {
		t.Errorf("blockNumber = %d, want 100", got)
	}
}
