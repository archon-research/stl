package entity

import (
	"fmt"
	"math/big"
	"time"
)

// MorphoVaultState represents a snapshot of a MetaMorpho vault's state at a specific block.
type MorphoVaultState struct {
	ID             int64
	MorphoVaultID  int64
	BlockNumber    int64
	BlockVersion   int
	BlockTimestamp time.Time
	TotalAssets    *big.Int
	TotalShares    *big.Int
	// AccrueInterest raw data (nil when not triggered by AccrueInterest)
	FeeShares      *big.Int // V1: single fee, V2: performanceFeeShares
	NewTotalAssets *big.Int
	// V2-only AccrueInterest fields
	PreviousTotalAssets *big.Int
	ManagementFeeShares *big.Int
}

// NewMorphoVaultState creates a new MorphoVaultState entity with validation.
func NewMorphoVaultState(morphoVaultID, blockNumber int64, blockVersion int, timestamp time.Time, totalAssets, totalShares *big.Int) (*MorphoVaultState, error) {
	s := &MorphoVaultState{
		MorphoVaultID:  morphoVaultID,
		BlockNumber:    blockNumber,
		BlockVersion:   blockVersion,
		BlockTimestamp: timestamp,
		TotalAssets:    totalAssets,
		TotalShares:    totalShares,
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return s, nil
}

// WithAccrueInterest sets the AccrueInterest event data on the vault state.
func (s *MorphoVaultState) WithAccrueInterest(feeShares, newTotalAssets, previousTotalAssets, managementFeeShares *big.Int) {
	s.FeeShares = feeShares
	s.NewTotalAssets = newTotalAssets
	s.PreviousTotalAssets = previousTotalAssets
	s.ManagementFeeShares = managementFeeShares
}

func (s *MorphoVaultState) Validate() error {
	if s.MorphoVaultID <= 0 {
		return fmt.Errorf("morphoVaultID must be positive, got %d", s.MorphoVaultID)
	}
	if s.BlockNumber <= 0 {
		return fmt.Errorf("blockNumber must be positive, got %d", s.BlockNumber)
	}
	if s.BlockVersion < 0 {
		return fmt.Errorf("blockVersion must be non-negative, got %d", s.BlockVersion)
	}
	if s.BlockTimestamp.IsZero() {
		return fmt.Errorf("blockTimestamp must not be zero")
	}
	if s.TotalAssets == nil {
		return fmt.Errorf("totalAssets must not be nil")
	}
	if s.TotalShares == nil {
		return fmt.Errorf("totalShares must not be nil")
	}
	return nil
}

// ComputeVaultAssets calculates vault user assets from shares: shares * totalAssets / totalSupply (round down).
// Returns 0 if totalSupply is zero.
func ComputeVaultAssets(shares, totalAssets, totalShares *big.Int) *big.Int {
	if shares == nil || totalAssets == nil || totalShares == nil || totalShares.Sign() == 0 {
		return new(big.Int)
	}
	num := new(big.Int).Mul(shares, totalAssets)
	return new(big.Int).Div(num, totalShares)
}
