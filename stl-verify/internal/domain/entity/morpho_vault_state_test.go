package entity

import (
	"math/big"
	"testing"
)

func TestNewMorphoVaultState(t *testing.T) {
	zero := big.NewInt(0)

	tests := []struct {
		name        string
		vaultID     int64
		block       int64
		version     int
		assets      *big.Int
		supply      *big.Int
		wantErr     bool
		errContains string
	}{
		{
			name: "valid state", vaultID: 1, block: 100, version: 0,
			assets: big.NewInt(1000000), supply: big.NewInt(1000000),
		},
		{
			name: "zero vault ID", vaultID: 0, block: 100, version: 0,
			assets: zero, supply: zero,
			wantErr: true, errContains: "morphoVaultID must be positive",
		},
		{
			name: "zero block", vaultID: 1, block: 0, version: 0,
			assets: zero, supply: zero,
			wantErr: true, errContains: "blockNumber must be positive",
		},
		{
			name: "negative version", vaultID: 1, block: 100, version: -1,
			assets: zero, supply: zero,
			wantErr: true, errContains: "blockVersion must be non-negative",
		},
		{
			name: "nil assets", vaultID: 1, block: 100, version: 0,
			assets: nil, supply: zero,
			wantErr: true, errContains: "totalAssets must not be nil",
		},
		{
			name: "nil supply", vaultID: 1, block: 100, version: 0,
			assets: zero, supply: nil,
			wantErr: true, errContains: "totalSupply must not be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMorphoVaultState(tt.vaultID, tt.block, tt.version, tt.assets, tt.supply)
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
			if got.MorphoVaultID != tt.vaultID {
				t.Errorf("MorphoVaultID = %d, want %d", got.MorphoVaultID, tt.vaultID)
			}
		})
	}
}

func TestMorphoVaultState_WithAccrueInterest(t *testing.T) {
	state, err := NewMorphoVaultState(1, 100, 0, big.NewInt(1000), big.NewInt(1000))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	feeShares := big.NewInt(10)
	newTotalAssets := big.NewInt(1100)
	state.WithAccrueInterest(feeShares, newTotalAssets)

	if state.FeeShares.Cmp(feeShares) != 0 {
		t.Errorf("FeeShares = %s, want %s", state.FeeShares, feeShares)
	}
	if state.NewTotalAssets.Cmp(newTotalAssets) != 0 {
		t.Errorf("NewTotalAssets = %s, want %s", state.NewTotalAssets, newTotalAssets)
	}
}

func TestComputeVaultAssets(t *testing.T) {
	tests := []struct {
		name        string
		shares      *big.Int
		totalAssets *big.Int
		totalSupply *big.Int
		want        *big.Int
	}{
		{
			name:        "1:1 ratio",
			shares:      big.NewInt(1000),
			totalAssets: big.NewInt(1000),
			totalSupply: big.NewInt(1000),
			want:        big.NewInt(1000),
		},
		{
			name:        "2:1 ratio",
			shares:      big.NewInt(1000),
			totalAssets: big.NewInt(2000),
			totalSupply: big.NewInt(1000),
			want:        big.NewInt(2000),
		},
		{
			name:        "rounds down",
			shares:      big.NewInt(1),
			totalAssets: big.NewInt(10),
			totalSupply: big.NewInt(3),
			want:        big.NewInt(3), // 1*10/3 = 3.33 -> 3
		},
		{
			name:        "zero total supply",
			shares:      big.NewInt(1000),
			totalAssets: big.NewInt(2000),
			totalSupply: big.NewInt(0),
			want:        big.NewInt(0),
		},
		{
			name:        "nil total supply",
			shares:      big.NewInt(1000),
			totalAssets: big.NewInt(2000),
			totalSupply: nil,
			want:        big.NewInt(0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ComputeVaultAssets(tt.shares, tt.totalAssets, tt.totalSupply)
			if got.Cmp(tt.want) != 0 {
				t.Errorf("ComputeVaultAssets() = %s, want %s", got, tt.want)
			}
		})
	}
}
