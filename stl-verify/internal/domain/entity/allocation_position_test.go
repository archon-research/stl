package entity

import (
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

func TestAllocationPosition_Validate(t *testing.T) {
	addr := common.HexToAddress("0x0102030405060708090a0b0c0d0e0f1011121314")

	valid := func() *AllocationPosition {
		return &AllocationPosition{
			ChainID:        1,
			TokenAddress:   addr,
			ProxyAddress:   addr,
			Balance:        big.NewInt(1),
			Direction:      "in",
			PrimeID:        1,
			BlockNumber:    100,
			CreatedAtBlock: 100,
			CreatedAt:      time.Unix(1, 0).UTC(),
		}
	}

	cases := []struct {
		name    string
		mut     func(*AllocationPosition)
		wantErr bool
	}{
		{"ok", func(*AllocationPosition) {}, false},
		{"missing created_at", func(p *AllocationPosition) { p.CreatedAt = time.Time{} }, true},
		{"missing chain", func(p *AllocationPosition) { p.ChainID = 0 }, true},
		{"missing token address", func(p *AllocationPosition) { p.TokenAddress = common.Address{} }, true},
		{"missing proxy address", func(p *AllocationPosition) { p.ProxyAddress = common.Address{} }, true},
		{"missing balance", func(p *AllocationPosition) { p.Balance = nil }, true},
		{"missing direction", func(p *AllocationPosition) { p.Direction = "" }, true},
		{"bad direction", func(p *AllocationPosition) { p.Direction = "sideways" }, true},
		{"missing prime", func(p *AllocationPosition) { p.PrimeID = 0 }, true},
		{"missing block number", func(p *AllocationPosition) { p.BlockNumber = 0 }, true},
		{"zero created_at_block", func(p *AllocationPosition) { p.CreatedAtBlock = 0 }, true},
		{"negative created_at_block", func(p *AllocationPosition) { p.CreatedAtBlock = -1 }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := valid()
			tc.mut(p)
			err := p.Validate()
			if tc.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
