package entity

import (
	"math/big"
	"testing"
)

func TestNewMorphoMarketState(t *testing.T) {
	zero := big.NewInt(0)

	tests := []struct {
		name        string
		marketID    int64
		block       int64
		version     int
		supAssets   *big.Int
		supShares   *big.Int
		borAssets   *big.Int
		borShares   *big.Int
		lastUpdate  int64
		fee         *big.Int
		wantErr     bool
		errContains string
	}{
		{
			name: "valid state", marketID: 1, block: 100, version: 0,
			supAssets: big.NewInt(1000), supShares: big.NewInt(1000),
			borAssets: big.NewInt(500), borShares: big.NewInt(500),
			lastUpdate: 1700000000, fee: zero,
		},
		{
			name: "zero market ID", marketID: 0, block: 100, version: 0,
			supAssets: zero, supShares: zero, borAssets: zero, borShares: zero,
			lastUpdate: 0, fee: zero,
			wantErr: true, errContains: "morphoMarketID must be positive",
		},
		{
			name: "zero block", marketID: 1, block: 0, version: 0,
			supAssets: zero, supShares: zero, borAssets: zero, borShares: zero,
			lastUpdate: 0, fee: zero,
			wantErr: true, errContains: "blockNumber must be positive",
		},
		{
			name: "negative version", marketID: 1, block: 100, version: -1,
			supAssets: zero, supShares: zero, borAssets: zero, borShares: zero,
			lastUpdate: 0, fee: zero,
			wantErr: true, errContains: "blockVersion must be non-negative",
		},
		{
			name: "nil supply assets", marketID: 1, block: 100, version: 0,
			supAssets: nil, supShares: zero, borAssets: zero, borShares: zero,
			lastUpdate: 0, fee: zero,
			wantErr: true, errContains: "totalSupplyAssets must not be nil",
		},
		{
			name: "nil supply shares", marketID: 1, block: 100, version: 0,
			supAssets: zero, supShares: nil, borAssets: zero, borShares: zero,
			lastUpdate: 0, fee: zero,
			wantErr: true, errContains: "totalSupplyShares must not be nil",
		},
		{
			name: "nil borrow assets", marketID: 1, block: 100, version: 0,
			supAssets: zero, supShares: zero, borAssets: nil, borShares: zero,
			lastUpdate: 0, fee: zero,
			wantErr: true, errContains: "totalBorrowAssets must not be nil",
		},
		{
			name: "nil borrow shares", marketID: 1, block: 100, version: 0,
			supAssets: zero, supShares: zero, borAssets: zero, borShares: nil,
			lastUpdate: 0, fee: zero,
			wantErr: true, errContains: "totalBorrowShares must not be nil",
		},
		{
			name: "nil fee", marketID: 1, block: 100, version: 0,
			supAssets: zero, supShares: zero, borAssets: zero, borShares: zero,
			lastUpdate: 0, fee: nil,
			wantErr: true, errContains: "fee must not be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMorphoMarketState(tt.marketID, tt.block, tt.version, tt.supAssets, tt.supShares, tt.borAssets, tt.borShares, tt.lastUpdate, tt.fee)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errContains != "" && !containsSubstring(err.Error(), tt.errContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.MorphoMarketID != tt.marketID {
				t.Errorf("MorphoMarketID = %d, want %d", got.MorphoMarketID, tt.marketID)
			}
		})
	}
}

func TestMorphoMarketState_WithAccrueInterest(t *testing.T) {
	zero := big.NewInt(0)
	state, err := NewMorphoMarketState(1, 100, 0, zero, zero, zero, zero, 0, zero)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rate := big.NewInt(1000)
	interest := big.NewInt(500)
	shares := big.NewInt(10)

	state.WithAccrueInterest(rate, interest, shares)

	if state.PrevBorrowRate.Cmp(rate) != 0 {
		t.Errorf("PrevBorrowRate = %s, want %s", state.PrevBorrowRate, rate)
	}
	if state.InterestAccrued.Cmp(interest) != 0 {
		t.Errorf("InterestAccrued = %s, want %s", state.InterestAccrued, interest)
	}
	if state.FeeShares.Cmp(shares) != 0 {
		t.Errorf("FeeShares = %s, want %s", state.FeeShares, shares)
	}
}
