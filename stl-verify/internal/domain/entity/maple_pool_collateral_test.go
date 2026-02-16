package entity

import (
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// ---------------------------------------------------------------------------
// TestNewMaplePoolCollateral
// ---------------------------------------------------------------------------

func TestNewMaplePoolCollateral(t *testing.T) {
	validAddr := common.HexToAddress("0x0000000000000000000000000000000000000001")
	validTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		id          int64
		poolAddr    common.Address
		poolName    string
		asset       string
		decimals    int
		assetValue  *big.Int
		poolTVL     *big.Int
		block       int64
		snapTime    time.Time
		wantErr     bool
		errContains string
	}{
		{
			name:       "valid collateral",
			poolAddr:   validAddr,
			poolName:   "Syrup USDC",
			asset:      "BTC",
			decimals:   8,
			assetValue: big.NewInt(674_000_000_000_000),
			poolTVL:    big.NewInt(1_000_000_000_000_000),
			block:      21000000,
			snapTime:   validTime,
		},
		{
			name:       "valid: zero asset value",
			poolAddr:   validAddr,
			poolName:   "Syrup USDC",
			asset:      "ETH",
			decimals:   18,
			assetValue: big.NewInt(0),
			poolTVL:    big.NewInt(1_000_000_000_000_000),
			block:      21000000,
			snapTime:   validTime,
		},
		{
			name:        "invalid: empty pool address",
			poolAddr:    common.Address{},
			poolName:    "Syrup USDC",
			asset:       "BTC",
			decimals:    8,
			assetValue:  big.NewInt(100),
			poolTVL:     big.NewInt(1000),
			block:       21000000,
			snapTime:    validTime,
			wantErr:     true,
			errContains: "pool address cannot be empty",
		},
		{
			name:        "invalid: empty pool name",
			poolAddr:    validAddr,
			poolName:    "",
			asset:       "BTC",
			decimals:    8,
			assetValue:  big.NewInt(100),
			poolTVL:     big.NewInt(1000),
			block:       21000000,
			snapTime:    validTime,
			wantErr:     true,
			errContains: "poolName must not be empty",
		},
		{
			name:        "invalid: empty asset",
			poolAddr:    validAddr,
			poolName:    "Syrup USDC",
			asset:       "",
			decimals:    8,
			assetValue:  big.NewInt(100),
			poolTVL:     big.NewInt(1000),
			block:       21000000,
			snapTime:    validTime,
			wantErr:     true,
			errContains: "asset must not be empty",
		},
		{
			name:        "invalid: nil asset value",
			poolAddr:    validAddr,
			poolName:    "Syrup USDC",
			asset:       "BTC",
			decimals:    8,
			assetValue:  nil,
			poolTVL:     big.NewInt(1000),
			block:       21000000,
			snapTime:    validTime,
			wantErr:     true,
			errContains: "assetValueUSD must not be nil",
		},
		{
			name:        "invalid: negative asset value",
			poolAddr:    validAddr,
			poolName:    "Syrup USDC",
			asset:       "BTC",
			decimals:    8,
			assetValue:  big.NewInt(-1),
			poolTVL:     big.NewInt(1000),
			block:       21000000,
			snapTime:    validTime,
			wantErr:     true,
			errContains: "assetValueUSD must be non-negative",
		},
		{
			name:        "invalid: nil pool TVL",
			poolAddr:    validAddr,
			poolName:    "Syrup USDC",
			asset:       "BTC",
			decimals:    8,
			assetValue:  big.NewInt(100),
			poolTVL:     nil,
			block:       21000000,
			snapTime:    validTime,
			wantErr:     true,
			errContains: "poolTVL must not be nil",
		},
		{
			name:        "invalid: zero pool TVL",
			poolAddr:    validAddr,
			poolName:    "Syrup USDC",
			asset:       "BTC",
			decimals:    8,
			assetValue:  big.NewInt(100),
			poolTVL:     big.NewInt(0),
			block:       21000000,
			snapTime:    validTime,
			wantErr:     true,
			errContains: "poolTVL must be positive",
		},
		{
			name:        "invalid: negative pool TVL",
			poolAddr:    validAddr,
			poolName:    "Syrup USDC",
			asset:       "BTC",
			decimals:    8,
			assetValue:  big.NewInt(100),
			poolTVL:     big.NewInt(-1000),
			block:       21000000,
			snapTime:    validTime,
			wantErr:     true,
			errContains: "poolTVL must be positive",
		},
		{
			name:        "invalid: block zero",
			poolAddr:    validAddr,
			poolName:    "Syrup USDC",
			asset:       "BTC",
			decimals:    8,
			assetValue:  big.NewInt(100),
			poolTVL:     big.NewInt(1000),
			block:       0,
			snapTime:    validTime,
			wantErr:     true,
			errContains: "snapshotBlock must be positive",
		},
		{
			name:        "invalid: zero snapshot time",
			poolAddr:    validAddr,
			poolName:    "Syrup USDC",
			asset:       "BTC",
			decimals:    8,
			assetValue:  big.NewInt(100),
			poolTVL:     big.NewInt(1000),
			block:       21000000,
			snapTime:    time.Time{},
			wantErr:     true,
			errContains: "snapshotTime must not be zero",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			col, err := NewMaplePoolCollateral(
				tc.id, tc.poolAddr, tc.poolName, tc.asset,
				tc.decimals, tc.assetValue, tc.poolTVL,
				tc.block, tc.snapTime,
			)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tc.errContains != "" {
					if got := err.Error(); !strings.Contains(got, tc.errContains) {
						t.Errorf("error %q should contain %q", got, tc.errContains)
					}
				}
				if col != nil {
					t.Error("collateral should be nil on error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if col == nil {
				t.Fatal("collateral is nil")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestMaplePoolCollateral_AssetPercent
// ---------------------------------------------------------------------------

func TestMaplePoolCollateral_AssetPercent(t *testing.T) {
	validAddr := common.HexToAddress("0x0000000000000000000000000000000000000001")
	validTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		assetValue  *big.Int
		poolTVL     *big.Int
		wantPercent float64
	}{
		{
			name:        "67.4% (BTC in Syrup USDC)",
			assetValue:  big.NewInt(674_000_000_000_000),
			poolTVL:     big.NewInt(1_000_000_000_000_000),
			wantPercent: 0.674,
		},
		{
			name:        "100% single collateral",
			assetValue:  big.NewInt(1_000_000_000_000_000),
			poolTVL:     big.NewInt(1_000_000_000_000_000),
			wantPercent: 1.0,
		},
		{
			name:        "0% zero value",
			assetValue:  big.NewInt(0),
			poolTVL:     big.NewInt(1_000_000_000_000_000),
			wantPercent: 0.0,
		},
		{
			name:        "50% exactly",
			assetValue:  big.NewInt(500_000_000_000_000),
			poolTVL:     big.NewInt(1_000_000_000_000_000),
			wantPercent: 0.5,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			col, err := NewMaplePoolCollateral(0, validAddr, "Test Pool", "BTC", 8, tc.assetValue, tc.poolTVL, 21000000, validTime)
			if err != nil {
				t.Fatalf("NewMaplePoolCollateral: %v", err)
			}

			pct, _ := col.AssetPercent().Float64()
			diff := pct - tc.wantPercent
			if diff < -0.001 || diff > 0.001 {
				t.Errorf("AssetPercent() = %f, want ~%f", pct, tc.wantPercent)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestMaplePoolCollateral_AssetValueFloat
// ---------------------------------------------------------------------------

func TestMaplePoolCollateral_AssetValueFloat(t *testing.T) {
	validAddr := common.HexToAddress("0x0000000000000000000000000000000000000001")
	validTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		assetValue *big.Int
		wantUSD    float64
	}{
		{
			name:       "$674M",
			assetValue: big.NewInt(674_000_000_000_000),
			wantUSD:    674_000_000.0,
		},
		{
			name:       "zero",
			assetValue: big.NewInt(0),
			wantUSD:    0.0,
		},
		{
			name:       "$1.23",
			assetValue: big.NewInt(1_230_000),
			wantUSD:    1.23,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			col, err := NewMaplePoolCollateral(0, validAddr, "Test Pool", "BTC", 8, tc.assetValue, big.NewInt(1_000_000_000_000_000), 21000000, validTime)
			if err != nil {
				t.Fatalf("NewMaplePoolCollateral: %v", err)
			}

			usd, _ := col.AssetValueFloat().Float64()
			diff := usd - tc.wantUSD
			if diff < -0.01 || diff > 0.01 {
				t.Errorf("AssetValueFloat() = %f, want ~%f", usd, tc.wantUSD)
			}
		})
	}
}
