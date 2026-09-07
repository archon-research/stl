package entity

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// PSM3ALMPosition is one tracked ALM proxy's LP stake in the pool, read in the
// same sweep as the reserve state. Which ALMs are tracked comes from the
// caller's config; the position carries its holder so a proxy rotation stays
// visible in the append-only history.
type PSM3ALMPosition struct {
	Prime      string         // registry name the stake belongs to (prime.name)
	Address    common.Address // ALM proxy the legs were read for
	Shares     *big.Int       // PSM3.shares(Address), raw 1e18 internal share units
	AssetValue *big.Int       // PSM3.convertToAssetValue(Shares) par valuation, raw 1e18
}

// PSM3State holds the raw reserve state of a Spark PSM3 deployment at one block.
type PSM3State struct {
	USDSBalance    *big.Int // USDS.balanceOf(psm3), raw 1e18
	SUSDSBalance   *big.Int // sUSDS.balanceOf(psm3), raw 1e18
	USDCBalance    *big.Int // USDC.balanceOf(pocket), raw 1e6
	TotalAssets    *big.Int // PSM3.totalAssets() par valuation, raw 1e18
	ConversionRate *big.Int // rateProvider().getConversionRate(), raw 1e27
	TotalShares    *big.Int // PSM3.totalShares(), raw 1e18 internal share units
	ALMPositions   []PSM3ALMPosition
}

// PSM3Reserves is a single sweep result: one append-only psm3_reserves row
// plus one psm3_alm_shares row per tracked ALM, all pinned to the same block.
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
		{"total_shares", s.State.TotalShares},
	}
	for _, f := range fields {
		if f.v == nil {
			return fmt.Errorf("%s is required", f.name)
		}
	}
	if len(s.State.ALMPositions) == 0 {
		return fmt.Errorf("at least one alm position is required")
	}
	seen := make(map[common.Address]bool, len(s.State.ALMPositions))
	for _, p := range s.State.ALMPositions {
		if err := p.validate(s.State); err != nil {
			return err
		}
		if seen[p.Address] {
			return fmt.Errorf("duplicate alm position for %s", p.Address.Hex())
		}
		seen[p.Address] = true
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

func (p *PSM3ALMPosition) validate(state PSM3State) error {
	if p.Prime == "" {
		return fmt.Errorf("alm position prime is required")
	}
	if p.Address == (common.Address{}) {
		return fmt.Errorf("alm position address is required (prime %s)", p.Prime)
	}
	if p.Shares == nil {
		return fmt.Errorf("shares is required (prime %s)", p.Prime)
	}
	if p.AssetValue == nil {
		return fmt.Errorf("asset_value is required (prime %s)", p.Prime)
	}
	// Both hold unconditionally for same-block reads (totalShares sums the
	// shares mapping; floor division is monotone), so a violation means the
	// caller paired call and result wrong — stop it before it is persisted.
	if p.Shares.Cmp(state.TotalShares) > 0 {
		return fmt.Errorf("shares %s exceeds total_shares %s (prime %s)", p.Shares, state.TotalShares, p.Prime)
	}
	if p.AssetValue.Cmp(state.TotalAssets) > 0 {
		return fmt.Errorf("asset_value %s exceeds total_assets %s (prime %s)", p.AssetValue, state.TotalAssets, p.Prime)
	}
	return nil
}
