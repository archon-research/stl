package postgres

import (
	"math/big"
	"slices"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// TestSharedPositionBlockNumberRejectsMixedBlocks guards the height bound
// readLatestPositionsV4 applies: a batch spanning blocks would compare a
// position against a row from a different block and silently skip the insert.
func TestSharedPositionBlockNumberRejectsMixedBlocks(t *testing.T) {
	rows := []*entity.UniswapV4Position{
		{PoolID: 1, TickLower: -60, TickUpper: 60, BlockNumber: 100, Liquidity: big.NewInt(1)},
		{PoolID: 1, TickLower: -120, TickUpper: 120, BlockNumber: 101, Liquidity: big.NewInt(1)},
	}
	blockNumberOf := func(p *entity.UniswapV4Position) int64 { return p.BlockNumber }
	if _, err := sharedBlockNumber("position", rows, blockNumberOf); err == nil {
		t.Fatal("sharedBlockNumber across two blocks: want error, got nil")
	}

	rows[1].BlockNumber = 100
	got, err := sharedBlockNumber("position", rows, blockNumberOf)
	if err != nil {
		t.Fatalf("sharedBlockNumber: %v", err)
	}
	if got != 100 {
		t.Errorf("blockNumber = %d, want 100", got)
	}
}

// TestV4PositionUnchanged pins the append-on-change decision, whose two halves
// are load-bearing for correctness: block_version separates a reorg
// re-observation from a redelivery, but only within one height — across heights
// the versions belong to different blocks and only the values may decide.
func TestV4PositionUnchanged(t *testing.T) {
	stored := func(blockNumber int64, blockVersion int, liquidity int64) v4PositionValues {
		return v4PositionValues{
			blockNumber:              blockNumber,
			blockVersion:             blockVersion,
			liquidity:                big.NewInt(liquidity),
			feeGrowthInside0LastX128: big.NewInt(2),
			feeGrowthInside1LastX128: big.NewInt(3),
		}
	}
	candidate := func(blockNumber int64, blockVersion int, liquidity int64) *entity.UniswapV4Position {
		return &entity.UniswapV4Position{
			PoolID:                   1,
			TickLower:                -60,
			TickUpper:                60,
			BlockNumber:              blockNumber,
			BlockVersion:             blockVersion,
			Liquidity:                big.NewInt(liquidity),
			FeeGrowthInside0LastX128: big.NewInt(2),
			FeeGrowthInside1LastX128: big.NewInt(3),
		}
	}

	for _, tc := range []struct {
		name  string
		prior v4PositionValues
		next  *entity.UniswapV4Position
		want  bool
	}{
		{"same_block_same_version_same_values", stored(100, 0, 1), candidate(100, 0, 1), true},
		{"same_block_reorg_version", stored(100, 0, 1), candidate(100, 1, 1), false},
		{"later_block_same_values", stored(100, 0, 1), candidate(101, 0, 1), true},
		{"later_block_after_reorg_same_values", stored(100, 1, 1), candidate(101, 0, 1), true},
		{"later_block_reorg_version_same_values", stored(100, 0, 1), candidate(101, 1, 1), true},
		{"later_block_changed_value", stored(100, 0, 1), candidate(101, 0, 2), false},
		{"earlier_stored_block_changed_value", stored(99, 0, 1), candidate(100, 0, 2), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := v4PositionUnchanged(tc.prior, tc.next); got != tc.want {
				t.Errorf("v4PositionUnchanged = %t, want %t", got, tc.want)
			}
		})
	}
}

// TestDistinctSortedV4PositionKeys pins the advisory-lock ordering for
// positions: every SaveBlock must request overlapping slots in the same
// sequence, or two concurrent block writers deadlock against each other.
func TestDistinctSortedV4PositionKeys(t *testing.T) {
	ownerA := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	ownerB := common.HexToAddress("0x00000000000000000000000000000000000000bb")
	saltA := common.HexToHash("0x01")
	saltB := common.HexToHash("0x02")

	position := func(poolID int64, owner common.Address, tickLower, tickUpper int, salt common.Hash) *entity.UniswapV4Position {
		return &entity.UniswapV4Position{
			PoolID: poolID, Owner: owner, TickLower: tickLower, TickUpper: tickUpper, Salt: salt,
		}
	}

	got := distinctSortedV4PositionKeys([]*entity.UniswapV4Position{
		position(7, ownerB, -60, 60, saltA),
		position(2, ownerA, -60, 60, saltB),
		position(2, ownerA, -60, 60, saltA),
		position(2, ownerA, -60, 60, saltB),
		position(2, ownerA, -120, 60, saltA),
		position(2, ownerB, -60, 60, saltA),
	})

	want := []v4PositionKey{
		{poolID: 2, key: entity.UniswapV4PositionKey{Owner: ownerA, TickLower: -120, TickUpper: 60, Salt: saltA}},
		{poolID: 2, key: entity.UniswapV4PositionKey{Owner: ownerA, TickLower: -60, TickUpper: 60, Salt: saltA}},
		{poolID: 2, key: entity.UniswapV4PositionKey{Owner: ownerA, TickLower: -60, TickUpper: 60, Salt: saltB}},
		{poolID: 2, key: entity.UniswapV4PositionKey{Owner: ownerB, TickLower: -60, TickUpper: 60, Salt: saltA}},
		{poolID: 7, key: entity.UniswapV4PositionKey{Owner: ownerB, TickLower: -60, TickUpper: 60, Salt: saltA}},
	}
	if !slices.Equal(got, want) {
		t.Errorf("keys = %+v, want %+v (duplicates must collapse)", got, want)
	}
}
