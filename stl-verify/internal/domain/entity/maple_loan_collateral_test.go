package entity

import (
	"math/big"
	"strings"
	"testing"
	"time"
)

func validMapleLoanCollateral() *MapleLoanCollateral {
	return &MapleLoanCollateral{
		MapleLoanID:      1,
		SyncedAt:         time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC),
		AssetSymbol:      "BTC",
		AssetAmount:      big.NewInt(215100000),
		AssetDecimals:    8,
		AssetValueUSD:    big.NewInt(6357500000),
		State:            "Deposited",
		Custodian:        "ANCHORAGE",
		LiquidationLevel: big.NewInt(1020000),
	}
}

func TestMapleLoanCollateral_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(c *MapleLoanCollateral)
		wantErr string
	}{
		{name: "valid collateral"},
		{
			name: "optional fields empty ok",
			mutate: func(c *MapleLoanCollateral) {
				c.State = ""
				c.Custodian = ""
				c.LiquidationLevel = nil
			},
		},
		{
			name:    "zero loan ID",
			mutate:  func(c *MapleLoanCollateral) { c.MapleLoanID = 0 },
			wantErr: "mapleLoanID must be positive",
		},
		{
			name:    "zero synced at",
			mutate:  func(c *MapleLoanCollateral) { c.SyncedAt = time.Time{} },
			wantErr: "syncedAt must not be zero",
		},
		{
			name:    "empty asset symbol",
			mutate:  func(c *MapleLoanCollateral) { c.AssetSymbol = "" },
			wantErr: "assetSymbol must not be empty",
		},
		{
			name:    "nil asset amount",
			mutate:  func(c *MapleLoanCollateral) { c.AssetAmount = nil },
			wantErr: "assetAmount must not be nil",
		},
		{
			name:    "negative asset amount",
			mutate:  func(c *MapleLoanCollateral) { c.AssetAmount = big.NewInt(-1) },
			wantErr: "assetAmount must be non-negative",
		},
		{
			name:    "negative asset decimals",
			mutate:  func(c *MapleLoanCollateral) { c.AssetDecimals = -1 },
			wantErr: "assetDecimals must be non-negative",
		},
		{
			name:    "nil asset value usd",
			mutate:  func(c *MapleLoanCollateral) { c.AssetValueUSD = nil },
			wantErr: "assetValueUSD must not be nil",
		},
		{
			name:    "negative asset value usd",
			mutate:  func(c *MapleLoanCollateral) { c.AssetValueUSD = big.NewInt(-1) },
			wantErr: "assetValueUSD must be non-negative",
		},
		{
			name:    "negative liquidation level",
			mutate:  func(c *MapleLoanCollateral) { c.LiquidationLevel = big.NewInt(-1) },
			wantErr: "liquidationLevel must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validMapleLoanCollateral()
			if tt.mutate != nil {
				tt.mutate(c)
			}
			err := c.Validate()
			if tt.wantErr != "" {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error %q should contain %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestNewMapleLoanCollateral_Constructor(t *testing.T) {
	v := validMapleLoanCollateral()

	got, err := NewMapleLoanCollateral(v.MapleLoanID, v.SyncedAt, v.AssetSymbol, v.AssetAmount, v.AssetDecimals, v.AssetValueUSD, v.State, v.Custodian, v.LiquidationLevel)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.AssetSymbol != v.AssetSymbol || got.Custodian != v.Custodian {
		t.Errorf("fields not set: %+v", got)
	}

	if _, err := NewMapleLoanCollateral(0, v.SyncedAt, v.AssetSymbol, v.AssetAmount, v.AssetDecimals, v.AssetValueUSD, v.State, v.Custodian, v.LiquidationLevel); err == nil {
		t.Fatal("expected constructor to propagate validation error")
	} else if !strings.Contains(err.Error(), "NewMapleLoanCollateral") {
		t.Errorf("error %q should be wrapped with constructor name", err.Error())
	}
}
