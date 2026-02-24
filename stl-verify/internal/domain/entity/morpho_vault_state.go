package entity

import (
	"fmt"
	"math/big"
)

// MorphoVaultState represents a snapshot of a MetaMorpho vault's state at a specific block.
type MorphoVaultState struct {
	ID            int64
	MorphoVaultID int64
	BlockNumber   int64
	BlockVersion  int
	TotalAssets   *big.Int
	TotalSupply   *big.Int // total vault shares
	// AccrueInterest raw data (nil when not triggered by AccrueInterest)
	FeeShares      *big.Int
	NewTotalAssets *big.Int
	// V2-only AccrueInterest fields
	Interest  *big.Int
	FeeAssets *big.Int
}

// NewMorphoVaultState creates a new MorphoVaultState entity with validation.
func NewMorphoVaultState(morphoVaultID, blockNumber int64, blockVersion int, totalAssets, totalSupply *big.Int) (*MorphoVaultState, error) {
	s := &MorphoVaultState{
		MorphoVaultID: morphoVaultID,
		BlockNumber:   blockNumber,
		BlockVersion:  blockVersion,
		TotalAssets:   totalAssets,
		TotalSupply:   totalSupply,
	}
	if err := s.validate(); err != nil {
		return nil, err
	}
	return s, nil
}

// WithAccrueInterest sets the AccrueInterest event data on the vault state.
func (s *MorphoVaultState) WithAccrueInterest(feeShares, newTotalAssets, interest, feeAssets *big.Int) {
	s.FeeShares = feeShares
	s.NewTotalAssets = newTotalAssets
	s.Interest = interest
	s.FeeAssets = feeAssets
}

func (s *MorphoVaultState) validate() error {
	if s.MorphoVaultID <= 0 {
		return fmt.Errorf("morphoVaultID must be positive, got %d", s.MorphoVaultID)
	}
	if s.BlockNumber <= 0 {
		return fmt.Errorf("blockNumber must be positive, got %d", s.BlockNumber)
	}
	if s.BlockVersion < 0 {
		return fmt.Errorf("blockVersion must be non-negative, got %d", s.BlockVersion)
	}
	if s.TotalAssets == nil {
		return fmt.Errorf("totalAssets must not be nil")
	}
	if s.TotalSupply == nil {
		return fmt.Errorf("totalSupply must not be nil")
	}
	return nil
}

// ComputeVaultAssets calculates vault user assets from shares: shares * totalAssets / totalSupply (round down).
// Returns 0 if totalSupply is zero.
func ComputeVaultAssets(shares, totalAssets, totalSupply *big.Int) *big.Int {
	if totalSupply == nil || totalSupply.Sign() == 0 {
		return new(big.Int)
	}
	num := new(big.Int).Mul(shares, totalAssets)
	return new(big.Int).Div(num, totalSupply)
}
