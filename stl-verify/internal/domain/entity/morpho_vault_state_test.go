package entity

import (
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestNewMorphoVaultState(t *testing.T) {
	zero := big.NewInt(0)
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		vaultID     int64
		block       int64
		version     int
		timestamp   time.Time
		assets      *big.Int
		supply      *big.Int
		wantErr     bool
		errContains string
	}{
		{
			name: "valid state", vaultID: 1, block: 100, version: 0, timestamp: ts,
			assets: big.NewInt(1000000), supply: big.NewInt(1000000),
		},
		{
			name: "zero vault ID", vaultID: 0, block: 100, version: 0, timestamp: ts,
			assets: zero, supply: zero,
			wantErr: true, errContains: "morphoVaultID must be positive",
		},
		{
			name: "zero block", vaultID: 1, block: 0, version: 0, timestamp: ts,
			assets: zero, supply: zero,
			wantErr: true, errContains: "blockNumber must be positive",
		},
		{
			name: "negative version", vaultID: 1, block: 100, version: -1, timestamp: ts,
			assets: zero, supply: zero,
			wantErr: true, errContains: "blockVersion must be non-negative",
		},
		{
			name: "zero timestamp", vaultID: 1, block: 100, version: 0, timestamp: time.Time{},
			assets: zero, supply: zero,
			wantErr: true, errContains: "timestamp must not be zero",
		},
		{
			name: "nil assets", vaultID: 1, block: 100, version: 0, timestamp: ts,
			assets: nil, supply: zero,
			wantErr: true, errContains: "totalAssets must not be nil",
		},
		{
			name: "nil supply", vaultID: 1, block: 100, version: 0, timestamp: ts,
			assets: zero, supply: nil,
			wantErr: true, errContains: "totalShares must not be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMorphoVaultState(tt.vaultID, tt.block, tt.version, tt.timestamp, tt.assets, tt.supply)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
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
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	state, err := NewMorphoVaultState(1, 100, 0, ts, big.NewInt(1000), big.NewInt(1000))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	feeShares := big.NewInt(10)
	newTotalAssets := big.NewInt(1100)
	previousTotalAssets := big.NewInt(1000)
	managementFeeShares := big.NewInt(5)
	state.WithAccrueInterest(feeShares, newTotalAssets, previousTotalAssets, managementFeeShares)

	if state.FeeShares.Cmp(feeShares) != 0 {
		t.Errorf("FeeShares = %s, want %s", state.FeeShares, feeShares)
	}
	if state.NewTotalAssets.Cmp(newTotalAssets) != 0 {
		t.Errorf("NewTotalAssets = %s, want %s", state.NewTotalAssets, newTotalAssets)
	}
	if state.PreviousTotalAssets.Cmp(previousTotalAssets) != 0 {
		t.Errorf("PreviousTotalAssets = %s, want %s", state.PreviousTotalAssets, previousTotalAssets)
	}
	if state.ManagementFeeShares.Cmp(managementFeeShares) != 0 {
		t.Errorf("ManagementFeeShares = %s, want %s", state.ManagementFeeShares, managementFeeShares)
	}
}

func TestComputeVaultAssets(t *testing.T) {
	tests := []struct {
		name        string
		shares      *big.Int
		totalAssets *big.Int
		totalShares *big.Int
		want        *big.Int
	}{
		{
			name:        "1:1 ratio",
			shares:      big.NewInt(1000),
			totalAssets: big.NewInt(1000),
			totalShares: big.NewInt(1000),
			want:        big.NewInt(1000),
		},
		{
			name:        "2:1 ratio",
			shares:      big.NewInt(1000),
			totalAssets: big.NewInt(2000),
			totalShares: big.NewInt(1000),
			want:        big.NewInt(2000),
		},
		{
			name:        "rounds down",
			shares:      big.NewInt(1),
			totalAssets: big.NewInt(10),
			totalShares: big.NewInt(3),
			want:        big.NewInt(3), // 1*10/3 = 3.33 -> 3
		},
		{
			name:        "zero total supply",
			shares:      big.NewInt(1000),
			totalAssets: big.NewInt(2000),
			totalShares: big.NewInt(0),
			want:        big.NewInt(0),
		},
		{
			name:        "nil total supply",
			shares:      big.NewInt(1000),
			totalAssets: big.NewInt(2000),
			totalShares: nil,
			want:        big.NewInt(0),
		},
		{
			name:        "nil shares",
			shares:      nil,
			totalAssets: big.NewInt(2000),
			totalShares: big.NewInt(1000),
			want:        big.NewInt(0),
		},
		{
			name:        "nil total assets",
			shares:      big.NewInt(1000),
			totalAssets: nil,
			totalShares: big.NewInt(1000),
			want:        big.NewInt(0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ComputeVaultAssets(tt.shares, tt.totalAssets, tt.totalShares)
			if got.Cmp(tt.want) != 0 {
				t.Errorf("ComputeVaultAssets() = %s, want %s", got, tt.want)
			}
		})
	}
}
