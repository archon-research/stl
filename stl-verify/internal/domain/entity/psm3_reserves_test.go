package entity

import (
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

func validPSM3Reserves() PSM3Reserves {
	return PSM3Reserves{
		ChainID: 8453,
		Address: common.HexToAddress("0x1601843c5E9bC251A3272907010AFa41Fa18347E"),
		State: PSM3State{
			USDSBalance:    big.NewInt(1),
			SUSDSBalance:   big.NewInt(2),
			USDCBalance:    big.NewInt(3),
			TotalAssets:    big.NewInt(4),
			ConversionRate: big.NewInt(5),
		},
		BlockNumber:    100,
		BlockVersion:   0,
		BlockTimestamp: time.Unix(1700000000, 0).UTC(),
		Source:         "sweep",
	}
}

func TestPSM3Reserves_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*PSM3Reserves)
		wantErr string
	}{
		{"valid", func(s *PSM3Reserves) {}, ""},
		{"event source not yet supported", func(s *PSM3Reserves) { s.Source = "event" }, "source"},
		{"zero chain id", func(s *PSM3Reserves) { s.ChainID = 0 }, "chain_id"},
		{"negative chain id", func(s *PSM3Reserves) { s.ChainID = -1 }, "chain_id"},
		{"zero address", func(s *PSM3Reserves) { s.Address = common.Address{} }, "address"},
		{"nil usds balance", func(s *PSM3Reserves) { s.State.USDSBalance = nil }, "usds_balance"},
		{"nil susds balance", func(s *PSM3Reserves) { s.State.SUSDSBalance = nil }, "susds_balance"},
		{"nil usdc balance", func(s *PSM3Reserves) { s.State.USDCBalance = nil }, "usdc_balance"},
		{"nil total assets", func(s *PSM3Reserves) { s.State.TotalAssets = nil }, "total_assets"},
		{"nil conversion rate", func(s *PSM3Reserves) { s.State.ConversionRate = nil }, "conversion_rate"},
		{"zero block number", func(s *PSM3Reserves) { s.BlockNumber = 0 }, "block_number"},
		{"zero timestamp", func(s *PSM3Reserves) { s.BlockTimestamp = time.Time{} }, "block_timestamp"},
		{"bad source", func(s *PSM3Reserves) { s.Source = "manual" }, "source"},
		{"empty source", func(s *PSM3Reserves) { s.Source = "" }, "source"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			snap := validPSM3Reserves()
			tc.mutate(&snap)
			err := snap.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}
