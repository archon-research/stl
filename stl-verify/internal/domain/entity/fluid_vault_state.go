package entity

import (
	"fmt"
	"math/big"
	"time"
)

// FluidVaultState is a per-vault aggregate snapshot at a single block: the
// vault's total collateral and total debt across all borrowers, plus the
// supply/borrow exchange prices and rates that drive them. One row per vault
// per block (per block_version on reorgs); per-borrower positions are
// deliberately not modelled (VEC-436).
//
// TotalCollateral and TotalDebt are raw on-chain integer amounts (the vault's
// own token decimals), stored unscaled. ExchangePrice and Rate fields are
// optional: they are read straight from Fluid's VaultResolver and are nil when
// the indexer did not capture them for a given snapshot.
//
// SupplyRate and BorrowRate are signed: Fluid stores them as int256 and a
// negative supply rate is a genuine on-chain value, not a capture error, so a
// negative rate must be stored verbatim rather than dropped. TotalCollateral,
// TotalDebt, and the exchange prices are unsigned on-chain quantities and stay
// non-negative.
type FluidVaultState struct {
	FluidVaultID        int64
	BlockNumber         int64
	BlockVersion        int
	Timestamp           time.Time
	TotalCollateral     *big.Int
	TotalDebt           *big.Int
	SupplyExchangePrice *big.Int
	BorrowExchangePrice *big.Int
	SupplyRate          *big.Int
	BorrowRate          *big.Int
}

type FluidVaultStateParams struct {
	FluidVaultID        int64
	BlockNumber         int64
	BlockVersion        int
	Timestamp           time.Time
	TotalCollateral     *big.Int
	TotalDebt           *big.Int
	SupplyExchangePrice *big.Int
	BorrowExchangePrice *big.Int
	SupplyRate          *big.Int
	BorrowRate          *big.Int
}

func NewFluidVaultState(p FluidVaultStateParams) (*FluidVaultState, error) {
	s := &FluidVaultState{
		FluidVaultID:        p.FluidVaultID,
		BlockNumber:         p.BlockNumber,
		BlockVersion:        p.BlockVersion,
		Timestamp:           p.Timestamp,
		TotalCollateral:     p.TotalCollateral,
		TotalDebt:           p.TotalDebt,
		SupplyExchangePrice: p.SupplyExchangePrice,
		BorrowExchangePrice: p.BorrowExchangePrice,
		SupplyRate:          p.SupplyRate,
		BorrowRate:          p.BorrowRate,
	}
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("NewFluidVaultState: %w", err)
	}
	return s, nil
}

func (s *FluidVaultState) Validate() error {
	if s.FluidVaultID <= 0 {
		return fmt.Errorf("fluidVaultID must be positive, got %d", s.FluidVaultID)
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
	if err := requireNonNegativeBigInt("totalCollateral", s.TotalCollateral); err != nil {
		return err
	}
	if err := requireNonNegativeBigInt("totalDebt", s.TotalDebt); err != nil {
		return err
	}
	for _, f := range []struct {
		name string
		v    *big.Int
	}{
		{"supplyExchangePrice", s.SupplyExchangePrice},
		{"borrowExchangePrice", s.BorrowExchangePrice},
	} {
		if f.v != nil && f.v.Sign() < 0 {
			return fmt.Errorf("%s must be non-negative, got %s", f.name, f.v)
		}
	}
	return nil
}

func requireNonNegativeBigInt(name string, v *big.Int) error {
	if v == nil {
		return fmt.Errorf("%s must not be nil", name)
	}
	if v.Sign() < 0 {
		return fmt.Errorf("%s must be non-negative, got %s", name, v)
	}
	return nil
}
