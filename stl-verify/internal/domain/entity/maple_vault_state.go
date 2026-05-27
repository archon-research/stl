package entity

import (
	"fmt"
	"math/big"
	"time"
)

// MapleVaultState is a per-block snapshot of a Syrup ERC-4626 vault.
//
// build_id and processing_version are assigned at the persistence layer
// (repository + DB trigger) rather than carried on the domain entity.
type MapleVaultState struct {
	MapleVaultID       int64
	BlockNumber        int64
	BlockVersion       int
	BlockTimestamp     time.Time
	TotalAssets        *big.Int
	TotalSupply        *big.Int
	SharePrice         *big.Int // convertToAssets(10^assetDecimals) — share-to-asset rate
	UnderlyingPriceUSD *big.Int // nullable; oracle pricing not joined in PR1
	SyrupPriceUSD      *big.Int // nullable; derived
}

// NewMapleVaultState creates a new MapleVaultState entity with validation.
func NewMapleVaultState(
	mapleVaultID, blockNumber int64,
	blockVersion int,
	timestamp time.Time,
	totalAssets, totalSupply, sharePrice *big.Int,
) (*MapleVaultState, error) {
	s := &MapleVaultState{
		MapleVaultID:   mapleVaultID,
		BlockNumber:    blockNumber,
		BlockVersion:   blockVersion,
		BlockTimestamp: timestamp,
		TotalAssets:    totalAssets,
		TotalSupply:    totalSupply,
		SharePrice:     sharePrice,
	}
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("NewMapleVaultState: %w", err)
	}
	return s, nil
}

// WithPrices attaches optional USD price fields (looked up off-band).
func (s *MapleVaultState) WithPrices(underlyingUSD, syrupUSD *big.Int) {
	s.UnderlyingPriceUSD = underlyingUSD
	s.SyrupPriceUSD = syrupUSD
}

// Validate ensures the entity satisfies all invariants.
func (s *MapleVaultState) Validate() error {
	if s.MapleVaultID <= 0 {
		return fmt.Errorf("mapleVaultID must be positive, got %d", s.MapleVaultID)
	}
	if s.BlockNumber < 0 {
		return fmt.Errorf("blockNumber must be non-negative, got %d", s.BlockNumber)
	}
	if s.BlockVersion < 0 {
		return fmt.Errorf("blockVersion must be non-negative, got %d", s.BlockVersion)
	}
	if s.BlockTimestamp.IsZero() {
		return fmt.Errorf("blockTimestamp must not be zero")
	}
	if s.TotalAssets == nil || s.TotalSupply == nil || s.SharePrice == nil {
		return fmt.Errorf("totalAssets/totalSupply/sharePrice must not be nil")
	}
	return nil
}
