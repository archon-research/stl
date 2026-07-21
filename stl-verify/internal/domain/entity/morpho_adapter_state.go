package entity

import (
	"fmt"
	"math/big"
	"time"
)

// MorphoAdapterState is a per-adapter snapshot at a single block: the assets a
// VaultV2 liquidity adapter reports it holds in its downstream venue. One row
// per adapter per block (per block_version on reorgs).
//
// RealAssets is the adapter's realAssets() reading, a raw on-chain uint256 in
// the vault's underlying asset base units (unscaled). It is an unsigned
// on-chain quantity, so it stays non-negative.
type MorphoAdapterState struct {
	MorphoAdapterID int64
	BlockNumber     int64
	BlockVersion    int
	Timestamp       time.Time
	RealAssets      *big.Int
}

// NewMorphoAdapterState creates a new MorphoAdapterState entity with validation.
func NewMorphoAdapterState(morphoAdapterID, blockNumber int64, blockVersion int, timestamp time.Time, realAssets *big.Int) (*MorphoAdapterState, error) {
	s := &MorphoAdapterState{
		MorphoAdapterID: morphoAdapterID,
		BlockNumber:     blockNumber,
		BlockVersion:    blockVersion,
		Timestamp:       timestamp,
		RealAssets:      realAssets,
	}
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("NewMorphoAdapterState: %w", err)
	}
	return s, nil
}

func (s *MorphoAdapterState) Validate() error {
	if s.MorphoAdapterID <= 0 {
		return fmt.Errorf("morphoAdapterID must be positive, got %d", s.MorphoAdapterID)
	}
	if s.BlockNumber <= 0 {
		return fmt.Errorf("blockNumber must be positive, got %d", s.BlockNumber)
	}
	if s.BlockVersion < 0 {
		return fmt.Errorf("blockVersion must be non-negative, got %d", s.BlockVersion)
	}
	if s.Timestamp.IsZero() {
		return fmt.Errorf("timestamp must not be zero")
	}
	return requireNonNegativeBigInt("realAssets", s.RealAssets)
}
