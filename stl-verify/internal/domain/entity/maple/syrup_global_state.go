package maple

import (
	"fmt"
	"math/big"
	"time"
)

// SyrupGlobalState is a snapshot of Maple's protocol-wide Syrup
// aggregates at a sync cycle. APY values use 30 decimals
// (46314953537216910976747498327 = 4.63%); TVL is in pool-asset decimals.
type SyrupGlobalState struct {
	ChainID         int64
	SyncedAt        time.Time
	TVL             *big.Int
	APY             *big.Int // 30 decimals
	CollateralAPY   *big.Int // 30 decimals
	PoolAPY         *big.Int // 30 decimals
	DripsYieldBoost *big.Int // nil when absent
}

// NewSyrupGlobalState creates a new SyrupGlobalState entity with validation.
func NewSyrupGlobalState(chainID int64, syncedAt time.Time, tvl, apy, collateralAPY, poolAPY, dripsYieldBoost *big.Int) (*SyrupGlobalState, error) {
	s := &SyrupGlobalState{
		ChainID:         chainID,
		SyncedAt:        NormalizeSyncedAt(syncedAt),
		TVL:             tvl,
		APY:             apy,
		CollateralAPY:   collateralAPY,
		PoolAPY:         poolAPY,
		DripsYieldBoost: dripsYieldBoost,
	}
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("NewSyrupGlobalState: %w", err)
	}
	return s, nil
}

// Validate checks that all fields have valid values.
func (s *SyrupGlobalState) Validate() error {
	if s.ChainID <= 0 {
		return fmt.Errorf("chainID must be positive, got %d", s.ChainID)
	}
	if s.SyncedAt.IsZero() {
		return fmt.Errorf("syncedAt must not be zero")
	}
	if err := requireNonNegBigInt("tvl", s.TVL); err != nil {
		return err
	}
	if err := requireNonNegBigInt("apy", s.APY); err != nil {
		return err
	}
	if err := requireNonNegBigInt("collateralAPY", s.CollateralAPY); err != nil {
		return err
	}
	if err := requireNonNegBigInt("poolAPY", s.PoolAPY); err != nil {
		return err
	}
	if err := requireNonNegBigIntIfSet("dripsYieldBoost", s.DripsYieldBoost); err != nil {
		return err
	}
	return nil
}
