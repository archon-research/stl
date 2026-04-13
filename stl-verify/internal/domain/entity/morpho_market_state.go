package entity

import (
	"fmt"
	"math/big"
	"time"
)

// MorphoMarketState represents a snapshot of a Morpho Blue market's state at a specific block.
type MorphoMarketState struct {
	ID                int64
	MorphoMarketID    int64
	BlockNumber       int64
	BlockVersion      int
	BlockTimestamp    time.Time
	TotalSupplyAssets *big.Int
	TotalSupplyShares *big.Int
	TotalBorrowAssets *big.Int
	TotalBorrowShares *big.Int
	LastUpdate        int64    // timestamp of last interest accrual
	Fee               *big.Int // protocol fee (scaled by 1e18)
	// AccrueInterest event data (nil when not triggered by AccrueInterest)
	PrevBorrowRate  *big.Int
	InterestAccrued *big.Int
	FeeShares       *big.Int
}

// NewMorphoMarketState creates a new MorphoMarketState entity with validation.
func NewMorphoMarketState(morphoMarketID, blockNumber int64, blockVersion int, timestamp time.Time, totalSupplyAssets, totalSupplyShares, totalBorrowAssets, totalBorrowShares *big.Int, lastUpdate int64, fee *big.Int) (*MorphoMarketState, error) {
	s := &MorphoMarketState{
		MorphoMarketID:    morphoMarketID,
		BlockNumber:       blockNumber,
		BlockVersion:      blockVersion,
		BlockTimestamp:    timestamp,
		TotalSupplyAssets: totalSupplyAssets,
		TotalSupplyShares: totalSupplyShares,
		TotalBorrowAssets: totalBorrowAssets,
		TotalBorrowShares: totalBorrowShares,
		LastUpdate:        lastUpdate,
		Fee:               fee,
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return s, nil
}

// WithAccrueInterest sets the AccrueInterest event data on the market state.
func (s *MorphoMarketState) WithAccrueInterest(prevBorrowRate, interestAccrued, feeShares *big.Int) {
	s.PrevBorrowRate = prevBorrowRate
	s.InterestAccrued = interestAccrued
	s.FeeShares = feeShares
}

func (s *MorphoMarketState) Validate() error {
	if s.MorphoMarketID <= 0 {
		return fmt.Errorf("morphoMarketID must be positive, got %d", s.MorphoMarketID)
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
	if s.TotalSupplyAssets == nil {
		return fmt.Errorf("totalSupplyAssets must not be nil")
	}
	if s.TotalSupplyShares == nil {
		return fmt.Errorf("totalSupplyShares must not be nil")
	}
	if s.TotalBorrowAssets == nil {
		return fmt.Errorf("totalBorrowAssets must not be nil")
	}
	if s.TotalBorrowShares == nil {
		return fmt.Errorf("totalBorrowShares must not be nil")
	}
	if s.Fee == nil {
		return fmt.Errorf("fee must not be nil")
	}
	return nil
}
