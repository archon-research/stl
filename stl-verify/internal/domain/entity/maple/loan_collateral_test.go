package maple

import (
	"math/big"
	"strings"
	"testing"
	"time"
)

func validLoanCollateral() *LoanCollateral {
	return &LoanCollateral{
		LoanID:           1,
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

func TestLoanCollateral_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(c *LoanCollateral)
		wantErr string
	}{
		{name: "valid collateral"},
		{
			name: "optional fields empty ok",
			mutate: func(c *LoanCollateral) {
				c.State = ""
				c.Custodian = ""
				c.LiquidationLevel = nil
			},
		},
		{
			name:    "zero loan ID",
			mutate:  func(c *LoanCollateral) { c.LoanID = 0 },
			wantErr: "mapleLoanID must be positive",
		},
		{
			name:    "zero synced at",
			mutate:  func(c *LoanCollateral) { c.SyncedAt = time.Time{} },
			wantErr: "syncedAt must not be zero",
		},
		{
			name:    "empty asset symbol",
			mutate:  func(c *LoanCollateral) { c.AssetSymbol = "" },
			wantErr: "assetSymbol must not be empty",
		},
		{
			// Nullable in the API schema (e.g. DepositPending): nil is valid.
			name:   "nil asset amount is valid",
			mutate: func(c *LoanCollateral) { c.AssetAmount = nil },
		},
		{
			name:    "negative asset amount",
			mutate:  func(c *LoanCollateral) { c.AssetAmount = big.NewInt(-1) },
			wantErr: "assetAmount must be non-negative",
		},
		{
			name:    "negative asset decimals",
			mutate:  func(c *LoanCollateral) { c.AssetDecimals = -1 },
			wantErr: "assetDecimals must be non-negative",
		},
		{
			// Nullable in the API schema (e.g. DepositPending): nil is valid.
			name:   "nil asset value usd is valid",
			mutate: func(c *LoanCollateral) { c.AssetValueUSD = nil },
		},
		{
			name:    "negative asset value usd",
			mutate:  func(c *LoanCollateral) { c.AssetValueUSD = big.NewInt(-1) },
			wantErr: "assetValueUSD must be non-negative",
		},
		{
			name:    "negative liquidation level",
			mutate:  func(c *LoanCollateral) { c.LiquidationLevel = big.NewInt(-1) },
			wantErr: "liquidationLevel must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validLoanCollateral()
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

func TestNewLoanCollateral_Constructor(t *testing.T) {
	v := validLoanCollateral()

	params := LoanCollateralParams{
		LoanID:           v.LoanID,
		SyncedAt:         v.SyncedAt,
		AssetSymbol:      v.AssetSymbol,
		AssetAmount:      v.AssetAmount,
		AssetDecimals:    v.AssetDecimals,
		AssetValueUSD:    v.AssetValueUSD,
		State:            v.State,
		Custodian:        v.Custodian,
		LiquidationLevel: v.LiquidationLevel,
	}
	got, err := NewLoanCollateral(params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.AssetSymbol != v.AssetSymbol || got.Custodian != v.Custodian {
		t.Errorf("fields not set: %+v", got)
	}

	invalid := params
	invalid.LoanID = 0
	if _, err := NewLoanCollateral(invalid); err == nil {
		t.Fatal("expected constructor to propagate validation error")
	} else if !strings.Contains(err.Error(), "NewLoanCollateral") {
		t.Errorf("error %q should be wrapped with constructor name", err.Error())
	}
}
