package maple

import (
	"fmt"
	"math/big"
	"time"
)

// SkyStrategyState is a snapshot of a Sky strategy at a sync cycle.
// All big.Int values are raw API integers in pool-asset decimals.
type SkyStrategyState struct {
	SkyStrategyID      int64
	SyncedAt           time.Time
	State              string
	CurrentlyDeployed  *big.Int
	DepositedAssets    *big.Int
	WithdrawnAssets    *big.Int
	StrategyFeeRate    *big.Int // nil when absent
	TotalFeesCollected *big.Int // nil when absent
}

type SkyStrategyStateParams struct {
	SkyStrategyID      int64
	SyncedAt           time.Time
	State              string
	CurrentlyDeployed  *big.Int
	DepositedAssets    *big.Int
	WithdrawnAssets    *big.Int
	StrategyFeeRate    *big.Int
	TotalFeesCollected *big.Int
}

// NewSkyStrategyState creates a new SkyStrategyState entity with validation.
func NewSkyStrategyState(p SkyStrategyStateParams) (*SkyStrategyState, error) {
	s := &SkyStrategyState{
		SkyStrategyID:      p.SkyStrategyID,
		SyncedAt:           NormalizeSyncedAt(p.SyncedAt),
		State:              p.State,
		CurrentlyDeployed:  p.CurrentlyDeployed,
		DepositedAssets:    p.DepositedAssets,
		WithdrawnAssets:    p.WithdrawnAssets,
		StrategyFeeRate:    p.StrategyFeeRate,
		TotalFeesCollected: p.TotalFeesCollected,
	}
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("NewSkyStrategyState: %w", err)
	}
	return s, nil
}

// Validate checks that all fields have valid values.
func (s *SkyStrategyState) Validate() error {
	if s.SkyStrategyID <= 0 {
		return fmt.Errorf("mapleSkyStrategyID must be positive, got %d", s.SkyStrategyID)
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
