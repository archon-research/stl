package entity

import (
	"math/big"
	"strings"
	"testing"
)

func TestNewMorphoMarketPosition(t *testing.T) {
	zero := big.NewInt(0)

	tests := []struct {
		name        string
		userID      int64
		marketID    int64
		block       int64
		version     int
		supShares   *big.Int
		borShares   *big.Int
		collateral  *big.Int
		supAssets   *big.Int
		borAssets   *big.Int
		wantErr     bool
		errContains string
	}{
		{
			name: "valid position", userID: 1, marketID: 1, block: 100, version: 0,
			supShares: big.NewInt(1000), borShares: zero, collateral: big.NewInt(500),
			supAssets: big.NewInt(1050), borAssets: zero,
		},
		{
			name: "zero user ID", userID: 0, marketID: 1, block: 100, version: 0,
			supShares: zero, borShares: zero, collateral: zero,
			supAssets: zero, borAssets: zero,
			wantErr: true, errContains: "userID must be positive",
		},
		{
			name: "zero market ID", userID: 1, marketID: 0, block: 100, version: 0,
			supShares: zero, borShares: zero, collateral: zero,
			supAssets: zero, borAssets: zero,
			wantErr: true, errContains: "morphoMarketID must be positive",
		},
		{
			name: "nil supply shares", userID: 1, marketID: 1, block: 100, version: 0,
			supShares: nil, borShares: zero, collateral: zero,
			supAssets: zero, borAssets: zero,
			wantErr: true, errContains: "supplyShares must not be nil",
		},
		{
			name: "nil borrow shares", userID: 1, marketID: 1, block: 100, version: 0,
			supShares: zero, borShares: nil, collateral: zero,
			supAssets: zero, borAssets: zero,
			wantErr: true, errContains: "borrowShares must not be nil",
		},
		{
			name: "nil collateral", userID: 1, marketID: 1, block: 100, version: 0,
			supShares: zero, borShares: zero, collateral: nil,
			supAssets: zero, borAssets: zero,
			wantErr: true, errContains: "collateral must not be nil",
		},
		{
			name: "nil supply assets", userID: 1, marketID: 1, block: 100, version: 0,
			supShares: zero, borShares: zero, collateral: zero,
			supAssets: nil, borAssets: zero,
			wantErr: true, errContains: "supplyAssets must not be nil",
		},
		{
			name: "nil borrow assets", userID: 1, marketID: 1, block: 100, version: 0,
			supShares: zero, borShares: zero, collateral: zero,
			supAssets: zero, borAssets: nil,
			wantErr: true, errContains: "borrowAssets must not be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMorphoMarketPosition(tt.userID, tt.marketID, tt.block, tt.version, tt.supShares, tt.borShares, tt.collateral, tt.supAssets, tt.borAssets)
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
			if got.UserID != tt.userID {
				t.Errorf("UserID = %d, want %d", got.UserID, tt.userID)
			}
		})
	}
}

func TestComputeSupplyAssets(t *testing.T) {
	tests := []struct {
		name        string
		shares      *big.Int
		totalAssets *big.Int
		totalShares *big.Int
		want        *big.Int
	}{
		{
			name:        "normal case",
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
			name:        "zero total shares",
			shares:      big.NewInt(1000),
			totalAssets: big.NewInt(2000),
			totalShares: big.NewInt(0),
			want:        big.NewInt(0),
		},
		{
			name:        "nil total shares",
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
		{
			name:        "zero shares",
			shares:      big.NewInt(0),
			totalAssets: big.NewInt(2000),
			totalShares: big.NewInt(1000),
			want:        big.NewInt(0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ComputeSupplyAssets(tt.shares, tt.totalAssets, tt.totalShares)
			if got.Cmp(tt.want) != 0 {
				t.Errorf("ComputeSupplyAssets() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestComputeBorrowAssets(t *testing.T) {
	tests := []struct {
		name        string
		shares      *big.Int
		totalAssets *big.Int
		totalShares *big.Int
		want        *big.Int
	}{
		{
			name:        "normal case",
			shares:      big.NewInt(1000),
			totalAssets: big.NewInt(2000),
			totalShares: big.NewInt(1000),
			want:        big.NewInt(2000),
		},
		{
			name:        "rounds up",
			shares:      big.NewInt(1),
			totalAssets: big.NewInt(10),
			totalShares: big.NewInt(3),
			want:        big.NewInt(4), // (1*10 + 3-1)/3 = 12/3 = 4
		},
		{
			name:        "zero total shares",
			shares:      big.NewInt(1000),
			totalAssets: big.NewInt(2000),
			totalShares: big.NewInt(0),
			want:        big.NewInt(0),
		},
		{
			name:        "nil total shares",
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
		{
			name:        "zero shares",
			shares:      big.NewInt(0),
			totalAssets: big.NewInt(2000),
			totalShares: big.NewInt(1000),
			want:        big.NewInt(0),
		},
		{
			name:        "exact division no rounding",
			shares:      big.NewInt(500),
			totalAssets: big.NewInt(2000),
			totalShares: big.NewInt(1000),
			want:        big.NewInt(1000), // (500*2000 + 999)/1000 = 1000999/1000 = 1000
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ComputeBorrowAssets(tt.shares, tt.totalAssets, tt.totalShares)
			if got.Cmp(tt.want) != 0 {
				t.Errorf("ComputeBorrowAssets() = %s, want %s", got, tt.want)
			}
		})
	}
}
