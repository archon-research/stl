package entity

import (
	"math/big"
	"strings"
	"testing"
	"time"
)

func validFluidVaultStateParams() FluidVaultStateParams {
	return FluidVaultStateParams{
		FluidVaultID:    1,
		BlockNumber:     19000000,
		BlockVersion:    0,
		Timestamp:       time.Unix(1700000000, 0).UTC(),
		TotalCollateral: big.NewInt(1000),
		TotalDebt:       big.NewInt(500),
	}
}

func TestNewFluidVaultState_Valid(t *testing.T) {
	p := validFluidVaultStateParams()
	p.SupplyExchangePrice = big.NewInt(1_000_000_000_000)
	p.BorrowExchangePrice = big.NewInt(1_000_000_000_001)
	p.SupplyRate = big.NewInt(300)
	p.BorrowRate = big.NewInt(500)

	s, err := NewFluidVaultState(p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.TotalCollateral.Cmp(big.NewInt(1000)) != 0 {
		t.Errorf("TotalCollateral = %s, want 1000", s.TotalCollateral)
	}
	if s.BorrowRate.Cmp(big.NewInt(500)) != 0 {
		t.Errorf("BorrowRate = %s, want 500", s.BorrowRate)
	}
}

// TestNewFluidVaultState_NegativeRatesValid: Fluid rates are int256 and can be
// genuinely negative, so a negative supply/borrow rate must be accepted and
// stored verbatim rather than rejected.
func TestNewFluidVaultState_NegativeRatesValid(t *testing.T) {
	p := validFluidVaultStateParams()
	p.SupplyRate = big.NewInt(-300)
	p.BorrowRate = big.NewInt(-1)

	s, err := NewFluidVaultState(p)
	if err != nil {
		t.Fatalf("negative rates must be valid, got: %v", err)
	}
	if s.SupplyRate.Cmp(big.NewInt(-300)) != 0 {
		t.Errorf("SupplyRate = %s, want -300", s.SupplyRate)
	}
	if s.BorrowRate.Cmp(big.NewInt(-1)) != 0 {
		t.Errorf("BorrowRate = %s, want -1", s.BorrowRate)
	}
}

func TestNewFluidVaultState_OptionalFieldsNil(t *testing.T) {
	s, err := NewFluidVaultState(validFluidVaultStateParams())
	if err != nil {
		t.Fatalf("unexpected error with nil optional fields: %v", err)
	}
	if s.SupplyExchangePrice != nil || s.BorrowRate != nil {
		t.Errorf("expected optional fields to stay nil")
	}
}

func TestNewFluidVaultState_Invalid(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(*FluidVaultStateParams)
		errContains string
	}{
		{"zero vault id", func(p *FluidVaultStateParams) { p.FluidVaultID = 0 }, "fluidVaultID must be positive"},
		{"zero block", func(p *FluidVaultStateParams) { p.BlockNumber = 0 }, "blockNumber must be positive"},
		{"negative block version", func(p *FluidVaultStateParams) { p.BlockVersion = -1 }, "blockVersion must be non-negative"},
		{"zero timestamp", func(p *FluidVaultStateParams) { p.Timestamp = time.Time{} }, "timestamp must not be zero"},
		{"nil collateral", func(p *FluidVaultStateParams) { p.TotalCollateral = nil }, "totalCollateral must not be nil"},
		{"negative collateral", func(p *FluidVaultStateParams) { p.TotalCollateral = big.NewInt(-1) }, "totalCollateral must be non-negative"},
		{"nil debt", func(p *FluidVaultStateParams) { p.TotalDebt = nil }, "totalDebt must not be nil"},
		{"negative debt", func(p *FluidVaultStateParams) { p.TotalDebt = big.NewInt(-1) }, "totalDebt must be non-negative"},
		{"negative supply exchange price", func(p *FluidVaultStateParams) { p.SupplyExchangePrice = big.NewInt(-1) }, "supplyExchangePrice must be non-negative"},
		{"negative borrow exchange price", func(p *FluidVaultStateParams) { p.BorrowExchangePrice = big.NewInt(-1) }, "borrowExchangePrice must be non-negative"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := validFluidVaultStateParams()
			tt.mutate(&p)
			_, err := NewFluidVaultState(p)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.errContains) {
				t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
			}
		})
	}
}
