package entity

import (
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// ---------------------------------------------------------------------------
// TestNewMaplePosition
// ---------------------------------------------------------------------------

func TestNewMaplePosition(t *testing.T) {
	validAddr := common.HexToAddress("0x0000000000000000000000000000000000000001")
	validTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		id          int64
		userID      int64
		protocolID  int64
		poolAddr    common.Address
		poolName    string
		asset       string
		decimals    int
		balance     *big.Int
		block       int64
		snapTime    time.Time
		wantErr     bool
		errContains string
	}{
		{
			name:       "valid position",
			id:         0,
			userID:     1,
			protocolID: 1,
			poolAddr:   validAddr,
			poolName:   "Syrup USDC",
			asset:      "USDC",
			decimals:   6,
			balance:    big.NewInt(200_000_000_000_000),
			block:      21000000,
			snapTime:   validTime,
		},
		{
			name:       "valid with zero balance",
			id:         0,
			userID:     1,
			protocolID: 1,
			poolAddr:   validAddr,
			poolName:   "Syrup USDC",
			asset:      "USDC",
			decimals:   6,
			balance:    big.NewInt(0),
			block:      21000000,
			snapTime:   validTime,
		},
		{
			name:        "invalid: userID zero",
			userID:      0,
			protocolID:  1,
			poolAddr:    validAddr,
			poolName:    "Syrup USDC",
			asset:       "USDC",
			decimals:    6,
			balance:     big.NewInt(100),
			block:       21000000,
			snapTime:    validTime,
			wantErr:     true,
			errContains: "userID must be positive",
		},
		{
			name:        "invalid: negative userID",
			userID:      -1,
			protocolID:  1,
			poolAddr:    validAddr,
			poolName:    "Syrup USDC",
			asset:       "USDC",
			decimals:    6,
			balance:     big.NewInt(100),
			block:       21000000,
			snapTime:    validTime,
			wantErr:     true,
			errContains: "userID must be positive",
		},
		{
			name:        "invalid: protocolID zero",
			userID:      1,
			protocolID:  0,
			poolAddr:    validAddr,
			poolName:    "Syrup USDC",
			asset:       "USDC",
			decimals:    6,
			balance:     big.NewInt(100),
			block:       21000000,
			snapTime:    validTime,
			wantErr:     true,
			errContains: "protocolID must be positive",
		},
		{
			name:        "invalid: empty pool address",
			userID:      1,
			protocolID:  1,
			poolAddr:    common.Address{},
			poolName:    "Syrup USDC",
			asset:       "USDC",
			decimals:    6,
			balance:     big.NewInt(100),
			block:       21000000,
			snapTime:    validTime,
			wantErr:     true,
			errContains: "pool address cannot be empty",
		},
		{
			name:        "invalid: empty pool name",
			userID:      1,
			protocolID:  1,
			poolAddr:    validAddr,
			poolName:    "",
			asset:       "USDC",
			decimals:    6,
			balance:     big.NewInt(100),
			block:       21000000,
			snapTime:    validTime,
			wantErr:     true,
			errContains: "poolName must not be empty",
		},
		{
			name:        "invalid: empty asset symbol",
			userID:      1,
			protocolID:  1,
			poolAddr:    validAddr,
			poolName:    "Syrup USDC",
			asset:       "",
			decimals:    6,
			balance:     big.NewInt(100),
			block:       21000000,
			snapTime:    validTime,
			wantErr:     true,
			errContains: "assetSymbol must not be empty",
		},
		{
			name:        "invalid: nil balance",
			userID:      1,
			protocolID:  1,
			poolAddr:    validAddr,
			poolName:    "Syrup USDC",
			asset:       "USDC",
			decimals:    6,
			balance:     nil,
			block:       21000000,
			snapTime:    validTime,
			wantErr:     true,
			errContains: "lendingBalance must not be nil",
		},
		{
			name:        "invalid: negative balance",
			userID:      1,
			protocolID:  1,
			poolAddr:    validAddr,
			poolName:    "Syrup USDC",
			asset:       "USDC",
			decimals:    6,
			balance:     big.NewInt(-1),
			block:       21000000,
			snapTime:    validTime,
			wantErr:     true,
			errContains: "lendingBalance must be non-negative",
		},
		{
			name:        "invalid: block zero",
			userID:      1,
			protocolID:  1,
			poolAddr:    validAddr,
			poolName:    "Syrup USDC",
			asset:       "USDC",
			decimals:    6,
			balance:     big.NewInt(100),
			block:       0,
			snapTime:    validTime,
			wantErr:     true,
			errContains: "snapshotBlock must be positive",
		},
		{
			name:        "invalid: negative block",
			userID:      1,
			protocolID:  1,
			poolAddr:    validAddr,
			poolName:    "Syrup USDC",
			asset:       "USDC",
			decimals:    6,
			balance:     big.NewInt(100),
			block:       -1,
			snapTime:    validTime,
			wantErr:     true,
			errContains: "snapshotBlock must be positive",
		},
		{
			name:        "invalid: zero snapshot time",
			userID:      1,
			protocolID:  1,
			poolAddr:    validAddr,
			poolName:    "Syrup USDC",
			asset:       "USDC",
			decimals:    6,
			balance:     big.NewInt(100),
			block:       21000000,
			snapTime:    time.Time{},
			wantErr:     true,
			errContains: "snapshotTime must not be zero",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pos, err := NewMaplePosition(
				tc.id, tc.userID, tc.protocolID,
				tc.poolAddr, tc.poolName, tc.asset,
				tc.decimals, tc.balance, tc.block, tc.snapTime,
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
				if pos != nil {
					t.Error("position should be nil on error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if pos == nil {
				t.Fatal("position is nil")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestMaplePosition_LendingBalanceUSD
// ---------------------------------------------------------------------------

func TestMaplePosition_LendingBalanceUSD(t *testing.T) {
	tests := []struct {
		name    string
		balance *big.Int
		wantUSD float64
	}{
		{
			name:    "$200M (200_000_000_000_000 / 1e6)",
			balance: big.NewInt(200_000_000_000_000),
			wantUSD: 200_000_000.0,
		},
		{
			name:    "zero balance",
			balance: big.NewInt(0),
			wantUSD: 0.0,
		},
		{
			name:    "small amount: $1.50",
			balance: big.NewInt(1_500_000),
			wantUSD: 1.5,
		},
	}

	validAddr := common.HexToAddress("0x0000000000000000000000000000000000000001")
	validTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pos, err := NewMaplePosition(0, 1, 1, validAddr, "Test", "USDC", 6, tc.balance, 21000000, validTime)
			if err != nil {
				t.Fatalf("NewMaplePosition: %v", err)
			}

			usd, _ := pos.LendingBalanceUSD().Float64()
			diff := usd - tc.wantUSD
			if diff < -0.01 || diff > 0.01 {
				t.Errorf("LendingBalanceUSD() = %f, want ~%f", usd, tc.wantUSD)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestMaplePosition_PoolAddressHex
// ---------------------------------------------------------------------------

func TestMaplePosition_PoolAddressHex(t *testing.T) {
	addr := common.HexToAddress("0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b")
	validTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	pos, err := NewMaplePosition(0, 1, 1, addr, "Test", "USDC", 6, big.NewInt(100), 21000000, validTime)
	if err != nil {
		t.Fatalf("NewMaplePosition: %v", err)
	}

	got := pos.PoolAddress.Hex()
	want := "0x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b"
	if got != want {
		t.Errorf("PoolAddress.Hex() = %q, want %q", got, want)
	}
}
