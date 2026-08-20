package entity

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// PSM3State holds the raw reserve state of a Spark PSM3 deployment at one block.
type PSM3State struct {
	USDSBalance        *big.Int // USDS.balanceOf(psm3), raw 1e18
	SUSDSBalance       *big.Int // sUSDS.balanceOf(psm3), raw 1e18
	USDCBalance        *big.Int // USDC.balanceOf(pocket), raw 1e6
	TotalAssets        *big.Int // PSM3.totalAssets() par valuation, raw 1e18
	ConversionRate     *big.Int // rateProvider().getConversionRate(), raw 1e27
	SparkALMShares     *big.Int // PSM3.shares(almProxy), raw 1e18 internal share units
	TotalShares        *big.Int // PSM3.totalShares(), raw 1e18 internal share units
	SparkALMAssetValue *big.Int // PSM3.convertToAssetValue(shares(almProxy)) par valuation, raw 1e18
}

// PSM3Reserves is a single append-only psm3_reserves row.
type PSM3Reserves struct {
	ChainID int64
	Address common.Address // PSM3 contract
	// SparkALMAddress records which holder the share legs were read for.
	// Config-sourced, so a proxy rotation with a lagging config stays
	// distinguishable from a real divestment in the append-only history.
	SparkALMAddress common.Address
	State           PSM3State
	BlockNumber     int64
	BlockVersion    int
	BlockTimestamp  time.Time
	Source          string // "sweep"; widened when the event-driven path lands
}

// Validate checks that the snapshot is well-formed before persistence.
func (s *PSM3Reserves) Validate() error {
	if s.ChainID <= 0 {
		return fmt.Errorf("chain_id must be positive")
	}
	if s.Address == (common.Address{}) {
		return fmt.Errorf("address is required")
	}
	if s.SparkALMAddress == (common.Address{}) {
		return fmt.Errorf("spark_alm_address is required")
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
		{"spark_alm_shares", s.State.SparkALMShares},
		{"total_shares", s.State.TotalShares},
		{"spark_alm_asset_value", s.State.SparkALMAssetValue},
	}
	for _, f := range fields {
		if f.v == nil {
			return fmt.Errorf("%s is required", f.name)
		}
	}
	// Both hold unconditionally for same-block reads (totalShares sums the
	// shares mapping; floor division is monotone), so a violation means the
	// caller paired call and result wrong — stop it before it is persisted.
	if s.State.SparkALMShares.Cmp(s.State.TotalShares) > 0 {
		return fmt.Errorf("spark_alm_shares %s exceeds total_shares %s", s.State.SparkALMShares, s.State.TotalShares)
	}
	if s.State.SparkALMAssetValue.Cmp(s.State.TotalAssets) > 0 {
		return fmt.Errorf("spark_alm_asset_value %s exceeds total_assets %s", s.State.SparkALMAssetValue, s.State.TotalAssets)
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
