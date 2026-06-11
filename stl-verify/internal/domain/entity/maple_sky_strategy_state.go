package entity

import (
	"fmt"
	"math/big"
	"time"
)

// MapleSkyStrategyState is a snapshot of a Sky strategy at a sync cycle.
// All big.Int values are raw API integers in pool-asset decimals.
type MapleSkyStrategyState struct {
	MapleSkyStrategyID int64
	SyncedAt           time.Time
	State              string
	CurrentlyDeployed  *big.Int
	DepositedAssets    *big.Int
	WithdrawnAssets    *big.Int
	StrategyFeeRate    *big.Int // nil when absent
	TotalFeesCollected *big.Int // nil when absent
}

// NewMapleSkyStrategyState creates a new MapleSkyStrategyState entity with validation.
func NewMapleSkyStrategyState(mapleSkyStrategyID int64, syncedAt time.Time, state string, currentlyDeployed, depositedAssets, withdrawnAssets, strategyFeeRate, totalFeesCollected *big.Int) (*MapleSkyStrategyState, error) {
	s := &MapleSkyStrategyState{
		MapleSkyStrategyID: mapleSkyStrategyID,
		SyncedAt:           NormalizeSyncedAt(syncedAt),
		State:              state,
		CurrentlyDeployed:  currentlyDeployed,
		DepositedAssets:    depositedAssets,
		WithdrawnAssets:    withdrawnAssets,
		StrategyFeeRate:    strategyFeeRate,
		TotalFeesCollected: totalFeesCollected,
	}
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("NewMapleSkyStrategyState: %w", err)
	}
	return s, nil
}

// Validate checks that all fields have valid values.
func (s *MapleSkyStrategyState) Validate() error {
	if s.MapleSkyStrategyID <= 0 {
		return fmt.Errorf("mapleSkyStrategyID must be positive, got %d", s.MapleSkyStrategyID)
	}
	if s.SyncedAt.IsZero() {
		return fmt.Errorf("syncedAt must not be zero")
	}
	if s.State == "" {
		return fmt.Errorf("state must not be empty")
	}
	if err := requireNonNegBigInt("currentlyDeployed", s.CurrentlyDeployed); err != nil {
		return err
	}
	if err := requireNonNegBigInt("depositedAssets", s.DepositedAssets); err != nil {
		return err
	}
	if err := requireNonNegBigInt("withdrawnAssets", s.WithdrawnAssets); err != nil {
		return err
	}
	if err := requireNonNegBigIntIfSet("strategyFeeRate", s.StrategyFeeRate); err != nil {
		return err
	}
	if err := requireNonNegBigIntIfSet("totalFeesCollected", s.TotalFeesCollected); err != nil {
		return err
	}
	return nil
}
