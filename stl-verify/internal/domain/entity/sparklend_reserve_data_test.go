package entity

import (
	"math/big"
	"strings"
	"testing"
)

func TestNewSparkLendReserveData(t *testing.T) {
	tests := []struct {
		name        string
		id          int64
		protocolID  int64
		tokenID     int64
		blockNumber int64
		wantErr     bool
		errContains string
	}{
		{
			name:        "valid reserve data",
			id:          1,
			protocolID:  5,
			tokenID:     10,
			blockNumber: 1000,
			wantErr:     false,
		},
		{
			name:        "zero id",
			id:          0,
			protocolID:  5,
			tokenID:     10,
			blockNumber: 1000,
			wantErr:     true,
			errContains: "id must be positive",
		},
		{
			name:        "negative id",
			id:          -1,
			protocolID:  5,
			tokenID:     10,
			blockNumber: 1000,
			wantErr:     true,
			errContains: "id must be positive",
		},
		{
			name:        "zero protocolID",
			id:          1,
			protocolID:  0,
			tokenID:     10,
			blockNumber: 1000,
			wantErr:     true,
			errContains: "protocolID must be positive",
		},
		{
			name:        "zero tokenID",
			id:          1,
			protocolID:  5,
			tokenID:     0,
			blockNumber: 1000,
			wantErr:     true,
			errContains: "tokenID must be positive",
		},
		{
			name:        "zero blockNumber",
			id:          1,
			protocolID:  5,
			tokenID:     10,
			blockNumber: 0,
			wantErr:     true,
			errContains: "blockNumber must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srd, err := NewSparkLendReserveData(tt.id, tt.protocolID, tt.tokenID, tt.blockNumber)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewSparkLendReserveData() expected error, got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("NewSparkLendReserveData() error = %v, want error containing %v", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("NewSparkLendReserveData() unexpected error = %v", err)
				return
			}
			if srd == nil {
				t.Errorf("NewSparkLendReserveData() returned nil")
				return
			}
			if srd.ID != tt.id {
				t.Errorf("NewSparkLendReserveData() ID = %v, want %v", srd.ID, tt.id)
			}
			if srd.ProtocolID != tt.protocolID {
				t.Errorf("NewSparkLendReserveData() ProtocolID = %v, want %v", srd.ProtocolID, tt.protocolID)
			}
			if srd.TokenID != tt.tokenID {
				t.Errorf("NewSparkLendReserveData() TokenID = %v, want %v", srd.TokenID, tt.tokenID)
			}
			if srd.BlockNumber != tt.blockNumber {
				t.Errorf("NewSparkLendReserveData() BlockNumber = %v, want %v", srd.BlockNumber, tt.blockNumber)
			}
		})
	}
}

func TestSparkLendReserveData_WithRates(t *testing.T) {
	srd, err := NewSparkLendReserveData(1, 5, 10, 1000)
	if err != nil {
		t.Fatalf("NewSparkLendReserveData() unexpected error = %v", err)
	}

	liquidityRate := big.NewInt(1000)
	variableBorrowRate := big.NewInt(2000)
	stableBorrowRate := big.NewInt(3000)
	avgStableBorrowRate := big.NewInt(2500)

	result := srd.WithRates(liquidityRate, variableBorrowRate, stableBorrowRate, avgStableBorrowRate)

	if result != srd {
		t.Errorf("WithRates() should return same instance")
	}
	if srd.LiquidityRate.Cmp(liquidityRate) != 0 {
		t.Errorf("WithRates() LiquidityRate = %v, want %v", srd.LiquidityRate, liquidityRate)
	}
	if srd.VariableBorrowRate.Cmp(variableBorrowRate) != 0 {
		t.Errorf("WithRates() VariableBorrowRate = %v, want %v", srd.VariableBorrowRate, variableBorrowRate)
	}
	if srd.StableBorrowRate.Cmp(stableBorrowRate) != 0 {
		t.Errorf("WithRates() StableBorrowRate = %v, want %v", srd.StableBorrowRate, stableBorrowRate)
	}
	if srd.AverageStableBorrowRate.Cmp(avgStableBorrowRate) != 0 {
		t.Errorf("WithRates() AverageStableBorrowRate = %v, want %v", srd.AverageStableBorrowRate, avgStableBorrowRate)
	}
}

func TestSparkLendReserveData_WithIndexes(t *testing.T) {
	srd, err := NewSparkLendReserveData(1, 5, 10, 1000)
	if err != nil {
		t.Fatalf("NewSparkLendReserveData() unexpected error = %v", err)
	}

	liquidityIndex := big.NewInt(5000)
	variableBorrowIndex := big.NewInt(6000)

	result := srd.WithIndexes(liquidityIndex, variableBorrowIndex)

	if result != srd {
		t.Errorf("WithIndexes() should return same instance")
	}
	if srd.LiquidityIndex.Cmp(liquidityIndex) != 0 {
		t.Errorf("WithIndexes() LiquidityIndex = %v, want %v", srd.LiquidityIndex, liquidityIndex)
	}
	if srd.VariableBorrowIndex.Cmp(variableBorrowIndex) != 0 {
		t.Errorf("WithIndexes() VariableBorrowIndex = %v, want %v", srd.VariableBorrowIndex, variableBorrowIndex)
	}
}

func TestSparkLendReserveData_WithTotals(t *testing.T) {
	srd, err := NewSparkLendReserveData(1, 5, 10, 1000)
	if err != nil {
		t.Fatalf("NewSparkLendReserveData() unexpected error = %v", err)
	}

	unbacked := big.NewInt(100)
	accruedToTreasury := big.NewInt(200)
	totalAToken := big.NewInt(10000)
	totalStableDebt := big.NewInt(3000)
	totalVariableDebt := big.NewInt(7000)

	result := srd.WithTotals(unbacked, accruedToTreasury, totalAToken, totalStableDebt, totalVariableDebt)

	if result != srd {
		t.Errorf("WithTotals() should return same instance")
	}
	if srd.Unbacked.Cmp(unbacked) != 0 {
		t.Errorf("WithTotals() Unbacked = %v, want %v", srd.Unbacked, unbacked)
	}
	if srd.AccruedToTreasuryScaled.Cmp(accruedToTreasury) != 0 {
		t.Errorf("WithTotals() AccruedToTreasuryScaled = %v, want %v", srd.AccruedToTreasuryScaled, accruedToTreasury)
	}
	if srd.TotalAToken.Cmp(totalAToken) != 0 {
		t.Errorf("WithTotals() TotalAToken = %v, want %v", srd.TotalAToken, totalAToken)
	}
	if srd.TotalStableDebt.Cmp(totalStableDebt) != 0 {
		t.Errorf("WithTotals() TotalStableDebt = %v, want %v", srd.TotalStableDebt, totalStableDebt)
	}
	if srd.TotalVariableDebt.Cmp(totalVariableDebt) != 0 {
		t.Errorf("WithTotals() TotalVariableDebt = %v, want %v", srd.TotalVariableDebt, totalVariableDebt)
	}
}

func TestSparkLendReserveData_ChainedMethods(t *testing.T) {
	srd, err := NewSparkLendReserveData(1, 5, 10, 1000)
	if err != nil {
		t.Fatalf("NewSparkLendReserveData() unexpected error = %v", err)
	}

	// Test method chaining
	result := srd.
		WithRates(big.NewInt(1000), big.NewInt(2000), big.NewInt(3000), big.NewInt(2500)).
		WithIndexes(big.NewInt(5000), big.NewInt(6000)).
		WithTotals(big.NewInt(100), big.NewInt(200), big.NewInt(10000), big.NewInt(3000), big.NewInt(7000))

	if result != srd {
		t.Errorf("Chained methods should return same instance")
	}
	if srd.LiquidityRate.Cmp(big.NewInt(1000)) != 0 {
		t.Errorf("Chained methods failed to set LiquidityRate")
	}
	if srd.LiquidityIndex.Cmp(big.NewInt(5000)) != 0 {
		t.Errorf("Chained methods failed to set LiquidityIndex")
	}
	if srd.TotalAToken.Cmp(big.NewInt(10000)) != 0 {
		t.Errorf("Chained methods failed to set TotalAToken")
	}
}
