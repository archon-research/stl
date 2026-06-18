package maple

import (
	"fmt"
	"math/big"
	"time"
)

// PoolState is a snapshot of a Maple pool's lending metrics at a sync
// cycle. TVL, LiquidAssets and PrincipalOut are raw API integers in
// pool-asset decimals (6 for USDC/USDT); CollateralValueUSD is USD-denominated,
// not pool-asset-denominated; APYs use 30 decimals. Utilization is derived in the
// constructor as principal_out / (liquid_assets + principal_out), in [0, 1],
// so an inconsistent triple is not representable via NewPoolState.
type PoolState struct {
	PoolID             int64
	SyncedAt           time.Time
	TVL                *big.Int // nil when the API reports null (schema-nullable)
	LiquidAssets       *big.Int // poolV2.assets
	CollateralValueUSD *big.Int // nil when the API reports null (schema-nullable)
	PrincipalOut       *big.Int
	Utilization        float64
	MonthlyAPY         *big.Int // 30 decimals, nil when absent
	SpotAPY            *big.Int // 30 decimals, nil when absent
}

type PoolStateParams struct {
	PoolID             int64
	SyncedAt           time.Time
	TVL                *big.Int
	LiquidAssets       *big.Int
	CollateralValueUSD *big.Int
	PrincipalOut       *big.Int
	MonthlyAPY         *big.Int
	SpotAPY            *big.Int
}

// NewPoolState creates a new PoolState entity with validation.
// Utilization is computed from PrincipalOut and LiquidAssets.
func NewPoolState(p PoolStateParams) (*PoolState, error) {
	s := &PoolState{
		PoolID:             p.PoolID,
		SyncedAt:           NormalizeSyncedAt(p.SyncedAt),
		TVL:                p.TVL,
		LiquidAssets:       p.LiquidAssets,
		CollateralValueUSD: p.CollateralValueUSD,
		PrincipalOut:       p.PrincipalOut,
		Utilization:        computeUtilization(p.PrincipalOut, p.LiquidAssets),
		MonthlyAPY:         p.MonthlyAPY,
		SpotAPY:            p.SpotAPY,
	}
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("NewPoolState: %w", err)
	}
	return s, nil
}

// computeUtilization derives principal_out / (liquid_assets + principal_out),
// returning 0 for an empty pool (zero denominator) or nil inputs (rejected by
// Validate afterwards).
func computeUtilization(principalOut, liquidAssets *big.Int) float64 {
	if principalOut == nil || liquidAssets == nil {
		return 0
	}
	denominator := new(big.Int).Add(liquidAssets, principalOut)
	if denominator.Sign() == 0 {
		return 0
	}
	utilization, _ := new(big.Float).Quo(
		new(big.Float).SetInt(principalOut),
		new(big.Float).SetInt(denominator),
	).Float64()
	return utilization
}

// Validate checks that all fields have valid values.
func (s *PoolState) Validate() error {
	if s.PoolID <= 0 {
		return fmt.Errorf("maplePoolID must be positive, got %d", s.PoolID)
	}
	if s.SyncedAt.IsZero() {
		return fmt.Errorf("syncedAt must not be zero")
	}
	if err := requireNonNegBigIntIfSet("tvl", s.TVL); err != nil {
		return err
	}
	if err := requireNonNegBigInt("liquidAssets", s.LiquidAssets); err != nil {
		return err
	}
	if err := requireNonNegBigIntIfSet("collateralValueUSD", s.CollateralValueUSD); err != nil {
		return err
	}
	if err := requireNonNegBigInt("principalOut", s.PrincipalOut); err != nil {
		return err
	}
	if s.Utilization < 0 || s.Utilization > 1 {
		return fmt.Errorf("utilization must be in [0, 1], got %f", s.Utilization)
	}
	if err := requireNonNegBigIntIfSet("monthlyAPY", s.MonthlyAPY); err != nil {
		return err
	}
	if err := requireNonNegBigIntIfSet("spotAPY", s.SpotAPY); err != nil {
		return err
	}
	return nil
}
