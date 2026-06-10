package entity

import (
	"fmt"
	"math/big"
	"time"
)

// MaplePoolState is a snapshot of a Maple pool's lending metrics at a sync
// cycle. All big.Int values are raw API integers in pool-asset decimals
// (6 for USDC/USDT); APYs use 30 decimals. Utilization is derived by the
// indexer as principal_out / (liquid_assets + principal_out), in [0, 1].
type MaplePoolState struct {
	MaplePoolID        int64
	SyncedAt           time.Time
	TVL                *big.Int
	LiquidAssets       *big.Int // poolV2.assets
	CollateralValueUSD *big.Int
	PrincipalOut       *big.Int
	Utilization        float64
	MonthlyAPY         *big.Int // 30 decimals, nil when absent
	SpotAPY            *big.Int // 30 decimals, nil when absent
}

// NewMaplePoolState creates a new MaplePoolState entity with validation.
func NewMaplePoolState(maplePoolID int64, syncedAt time.Time, tvl, liquidAssets, collateralValueUSD, principalOut *big.Int, utilization float64, monthlyAPY, spotAPY *big.Int) (*MaplePoolState, error) {
	s := &MaplePoolState{
		MaplePoolID:        maplePoolID,
		SyncedAt:           syncedAt,
		TVL:                tvl,
		LiquidAssets:       liquidAssets,
		CollateralValueUSD: collateralValueUSD,
		PrincipalOut:       principalOut,
		Utilization:        utilization,
		MonthlyAPY:         monthlyAPY,
		SpotAPY:            spotAPY,
	}
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("NewMaplePoolState: %w", err)
	}
	return s, nil
}

// Validate checks that all fields have valid values.
func (s *MaplePoolState) Validate() error {
	if s.MaplePoolID <= 0 {
		return fmt.Errorf("maplePoolID must be positive, got %d", s.MaplePoolID)
	}
	if s.SyncedAt.IsZero() {
		return fmt.Errorf("syncedAt must not be zero")
	}
	if s.TVL == nil {
		return fmt.Errorf("tvl must not be nil")
	}
	if s.TVL.Sign() < 0 {
		return fmt.Errorf("tvl must be non-negative, got %s", s.TVL)
	}
	if s.LiquidAssets == nil {
		return fmt.Errorf("liquidAssets must not be nil")
	}
	if s.LiquidAssets.Sign() < 0 {
		return fmt.Errorf("liquidAssets must be non-negative, got %s", s.LiquidAssets)
	}
	if s.CollateralValueUSD == nil {
		return fmt.Errorf("collateralValueUSD must not be nil")
	}
	if s.CollateralValueUSD.Sign() < 0 {
		return fmt.Errorf("collateralValueUSD must be non-negative, got %s", s.CollateralValueUSD)
	}
	if s.PrincipalOut == nil {
		return fmt.Errorf("principalOut must not be nil")
	}
	if s.PrincipalOut.Sign() < 0 {
		return fmt.Errorf("principalOut must be non-negative, got %s", s.PrincipalOut)
	}
	if s.Utilization < 0 || s.Utilization > 1 {
		return fmt.Errorf("utilization must be in [0, 1], got %f", s.Utilization)
	}
	return nil
}
