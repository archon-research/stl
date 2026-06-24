package entity

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// PSM3State holds the raw reserve state of a Spark PSM3 deployment at one block.
type PSM3State struct {
	USDSBalance    *big.Int // USDS.balanceOf(psm3), raw 1e18
	SUSDSBalance   *big.Int // sUSDS.balanceOf(psm3), raw 1e18
	USDCBalance    *big.Int // USDC.balanceOf(pocket), raw 1e6
	TotalAssets    *big.Int // PSM3.totalAssets() par valuation, raw 1e18
	ConversionRate *big.Int // rateProvider().getConversionRate(), raw 1e27
}

// PSM3Reserves is a single append-only psm3_reserves row.
type PSM3Reserves struct {
	ChainID        int64
	Address        common.Address // PSM3 contract
	State          PSM3State
	BlockNumber    int64
	BlockVersion   int
	BlockTimestamp time.Time
	Source         string // "sweep"; widened when the event-driven path lands
}

// Validate checks that the snapshot is well-formed before persistence.
func (s *PSM3Reserves) Validate() error {
	if s.ChainID <= 0 {
		return fmt.Errorf("chain_id must be positive")
	}
	if s.Address == (common.Address{}) {
		return fmt.Errorf("address is required")
	}
	fields := []struct {
		name string
		v    *big.Int
	}{
		{"usds_balance", s.State.USDSBalance},
		{"susds_balance", s.State.SUSDSBalance},
		{"usdc_balance", s.State.USDCBalance},
		{"total_assets", s.State.TotalAssets},
		{"conversion_rate", s.State.ConversionRate},
	}
	for _, f := range fields {
		if f.v == nil {
			return fmt.Errorf("%s is required", f.name)
		}
	}
	if s.BlockNumber <= 0 {
		return fmt.Errorf("block_number must be positive")
	}
	if s.BlockTimestamp.IsZero() {
		return fmt.Errorf("block_timestamp is required")
	}
	if s.Source != "sweep" {
		return fmt.Errorf("source must be 'sweep', got %q", s.Source)
	}
	return nil
}
