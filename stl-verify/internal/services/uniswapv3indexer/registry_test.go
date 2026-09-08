package uniswapv3indexer

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

func TestRegisteredPoolsFromRows(t *testing.T) {
	rows := []outbound.UniswapV3PoolRow{
		{
			ID:             1,
			ProtocolID:     7,
			Address:        common.HexToAddress("0x1111111111111111111111111111111111111111"),
			Token0:         common.HexToAddress("0x2222222222222222222222222222222222222222"),
			Token1:         common.HexToAddress("0x3333333333333333333333333333333333333333"),
			Token0Decimals: 18,
			Token1Decimals: 6,
			Fee:            3000,
			TickSpacing:    60,
			DeployBlock:    123456,
		},
		{
			ID:             2,
			ProtocolID:     7,
			Address:        common.HexToAddress("0x4444444444444444444444444444444444444444"),
			Token0:         common.HexToAddress("0x5555555555555555555555555555555555555555"),
			Token1:         common.HexToAddress("0x6666666666666666666666666666666666666666"),
			Token0Decimals: 8,
			Token1Decimals: 18,
			Fee:            500,
			TickSpacing:    10,
			DeployBlock:    654321,
		},
	}

	got := RegisteredPoolsFromRows(rows)

	if len(got) != len(rows) {
		t.Fatalf("got %d pools, want %d", len(got), len(rows))
	}
	for i, row := range rows {
		want := RegisteredPool{
			ID:             row.ID,
			Address:        row.Address,
			Token0:         row.Token0,
			Token1:         row.Token1,
			Token0Decimals: row.Token0Decimals,
			Token1Decimals: row.Token1Decimals,
			Fee:            row.Fee,
			TickSpacing:    row.TickSpacing,
			DeployBlock:    row.DeployBlock,
		}
		if got[i] != want {
			t.Errorf("pool %d = %+v, want %+v", i, got[i], want)
		}
	}
}

func TestRegisteredPoolsFromRows_Empty(t *testing.T) {
	got := RegisteredPoolsFromRows(nil)
	if len(got) != 0 {
		t.Errorf("got %d pools, want 0 for nil input", len(got))
	}
}
