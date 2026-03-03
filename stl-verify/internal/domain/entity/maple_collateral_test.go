package entity

import (
	"math/big"
	"strings"
	"testing"
)

func TestNewMapleCollateral(t *testing.T) {
	validAmount := big.NewInt(1_000_000_000)
	validLiqLevel := big.NewInt(1_500_000)

	tests := []struct {
		name               string
		loanID             int64
		userID             int64
		protocolID         int64
		collateralAsset    string
		collateralDecimals int
		amount             *big.Int
		custodian          string
		state              string
		liquidationLevel   *big.Int
		blockNumber        int64
		blockVersion       int
		wantErr            bool
		errContains        string
	}{
		{
			name:               "valid maple collateral",
			loanID:             100,
			userID:             10,
			protocolID:         5,
			collateralAsset:    "BTC",
			collateralDecimals: 8,
			amount:             validAmount,
			custodian:          "ANCHORAGE",
			state:              "Deposited",
			liquidationLevel:   validLiqLevel,
			blockNumber:        21000000,
			blockVersion:       0,
		},
		{
			name:               "valid with nil liquidation level",
			loanID:             101,
			userID:             10,
			protocolID:         5,
			collateralAsset:    "SOL",
			collateralDecimals: 9,
			amount:             validAmount,
			custodian:          "FORDEFI",
			state:              "DepositPending",
			liquidationLevel:   nil,
			blockNumber:        21000000,
			blockVersion:       1,
		},
		{
			name:               "valid with empty custodian",
			loanID:             102,
			userID:             10,
			protocolID:         5,
			collateralAsset:    "XRP",
			collateralDecimals: 6,
			amount:             validAmount,
			custodian:          "",
			state:              "Deposited",
			liquidationLevel:   validLiqLevel,
			blockNumber:        21000000,
			blockVersion:       0,
		},
		{
			name:               "valid with empty state",
			loanID:             103,
			userID:             10,
			protocolID:         5,
			collateralAsset:    "BTC",
			collateralDecimals: 8,
			amount:             validAmount,
			custodian:          "ANCHORAGE",
			state:              "",
			liquidationLevel:   validLiqLevel,
			blockNumber:        21000000,
			blockVersion:       0,
		},
		{
			name:               "valid with zero amount",
			loanID:             104,
			userID:             10,
			protocolID:         5,
			collateralAsset:    "BTC",
			collateralDecimals: 8,
			amount:             big.NewInt(0),
			custodian:          "ANCHORAGE",
			state:              "Deposited",
			liquidationLevel:   validLiqLevel,
			blockNumber:        21000000,
			blockVersion:       0,
		},
		{
			name:               "valid with zero decimals",
			loanID:             105,
			userID:             10,
			protocolID:         5,
			collateralAsset:    "BTC",
			collateralDecimals: 0,
			amount:             validAmount,
			custodian:          "ANCHORAGE",
			state:              "Deposited",
			liquidationLevel:   validLiqLevel,
			blockNumber:        21000000,
			blockVersion:       0,
		},
		{
			name:               "valid with large amount",
			loanID:             106,
			userID:             1,
			protocolID:         1,
			collateralAsset:    "BTC",
			collateralDecimals: 8,
			amount:             new(big.Int).Exp(big.NewInt(10), big.NewInt(30), nil),
			custodian:          "ANCHORAGE",
			state:              "Deposited",
			liquidationLevel:   validLiqLevel,
			blockNumber:        21000000,
			blockVersion:       0,
		},
		{
			name:               "zero loanID",
			loanID:             0,
			userID:             10,
			protocolID:         5,
			collateralAsset:    "BTC",
			collateralDecimals: 8,
			amount:             validAmount,
			custodian:          "ANCHORAGE",
			state:              "Deposited",
			liquidationLevel:   validLiqLevel,
			blockNumber:        21000000,
			blockVersion:       0,
			wantErr:            true,
			errContains:        "loanID must be positive",
		},
		{
			name:               "negative loanID",
			loanID:             -1,
			userID:             10,
			protocolID:         5,
			collateralAsset:    "BTC",
			collateralDecimals: 8,
			amount:             validAmount,
			custodian:          "ANCHORAGE",
			state:              "Deposited",
			liquidationLevel:   validLiqLevel,
			blockNumber:        21000000,
			blockVersion:       0,
			wantErr:            true,
			errContains:        "loanID must be positive",
		},
		{
			name:               "zero userID",
			loanID:             100,
			userID:             0,
			protocolID:         5,
			collateralAsset:    "BTC",
			collateralDecimals: 8,
			amount:             validAmount,
			custodian:          "ANCHORAGE",
			state:              "Deposited",
			liquidationLevel:   validLiqLevel,
			blockNumber:        21000000,
			blockVersion:       0,
			wantErr:            true,
			errContains:        "userID must be positive",
		},
		{
			name:               "negative userID",
			loanID:             100,
			userID:             -1,
			protocolID:         5,
			collateralAsset:    "BTC",
			collateralDecimals: 8,
			amount:             validAmount,
			custodian:          "ANCHORAGE",
			state:              "Deposited",
			liquidationLevel:   validLiqLevel,
			blockNumber:        21000000,
			blockVersion:       0,
			wantErr:            true,
			errContains:        "userID must be positive",
		},
		{
			name:               "zero protocolID",
			loanID:             100,
			userID:             10,
			protocolID:         0,
			collateralAsset:    "BTC",
			collateralDecimals: 8,
			amount:             validAmount,
			custodian:          "ANCHORAGE",
			state:              "Deposited",
			liquidationLevel:   validLiqLevel,
			blockNumber:        21000000,
			blockVersion:       0,
			wantErr:            true,
			errContains:        "protocolID must be positive",
		},
		{
			name:               "negative protocolID",
			loanID:             100,
			userID:             10,
			protocolID:         -5,
			collateralAsset:    "BTC",
			collateralDecimals: 8,
			amount:             validAmount,
			custodian:          "ANCHORAGE",
			state:              "Deposited",
			liquidationLevel:   validLiqLevel,
			blockNumber:        21000000,
			blockVersion:       0,
			wantErr:            true,
			errContains:        "protocolID must be positive",
		},
		{
			name:               "empty collateralAsset",
			loanID:             100,
			userID:             10,
			protocolID:         5,
			collateralAsset:    "",
			collateralDecimals: 8,
			amount:             validAmount,
			custodian:          "ANCHORAGE",
			state:              "Deposited",
			liquidationLevel:   validLiqLevel,
			blockNumber:        21000000,
			blockVersion:       0,
			wantErr:            true,
			errContains:        "collateralAsset must not be empty",
		},
		{
			name:               "negative collateralDecimals",
			loanID:             100,
			userID:             10,
			protocolID:         5,
			collateralAsset:    "BTC",
			collateralDecimals: -1,
			amount:             validAmount,
			custodian:          "ANCHORAGE",
			state:              "Deposited",
			liquidationLevel:   validLiqLevel,
			blockNumber:        21000000,
			blockVersion:       0,
			wantErr:            true,
			errContains:        "collateralDecimals must be non-negative",
		},
		{
			name:               "nil amount",
			loanID:             100,
			userID:             10,
			protocolID:         5,
			collateralAsset:    "BTC",
			collateralDecimals: 8,
			amount:             nil,
			custodian:          "ANCHORAGE",
			state:              "Deposited",
			liquidationLevel:   validLiqLevel,
			blockNumber:        21000000,
			blockVersion:       0,
			wantErr:            true,
			errContains:        "amount must not be nil",
		},
		{
			name:               "negative amount",
			loanID:             100,
			userID:             10,
			protocolID:         5,
			collateralAsset:    "BTC",
			collateralDecimals: 8,
			amount:             big.NewInt(-100),
			custodian:          "ANCHORAGE",
			state:              "Deposited",
			liquidationLevel:   validLiqLevel,
			blockNumber:        21000000,
			blockVersion:       0,
			wantErr:            true,
			errContains:        "amount must be non-negative",
		},
		{
			name:               "zero blockNumber",
			loanID:             100,
			userID:             10,
			protocolID:         5,
			collateralAsset:    "BTC",
			collateralDecimals: 8,
			amount:             validAmount,
			custodian:          "ANCHORAGE",
			state:              "Deposited",
			liquidationLevel:   validLiqLevel,
			blockNumber:        0,
			blockVersion:       0,
			wantErr:            true,
			errContains:        "blockNumber must be positive",
		},
		{
			name:               "negative blockNumber",
			loanID:             100,
			userID:             10,
			protocolID:         5,
			collateralAsset:    "BTC",
			collateralDecimals: 8,
			amount:             validAmount,
			custodian:          "ANCHORAGE",
			state:              "Deposited",
			liquidationLevel:   validLiqLevel,
			blockNumber:        -1,
			blockVersion:       0,
			wantErr:            true,
			errContains:        "blockNumber must be positive",
		},
		{
			name:               "negative blockVersion",
			loanID:             100,
			userID:             10,
			protocolID:         5,
			collateralAsset:    "BTC",
			collateralDecimals: 8,
			amount:             validAmount,
			custodian:          "ANCHORAGE",
			state:              "Deposited",
			liquidationLevel:   validLiqLevel,
			blockNumber:        21000000,
			blockVersion:       -1,
			wantErr:            true,
			errContains:        "blockVersion must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc, err := NewMapleCollateral(tt.loanID, tt.userID, tt.protocolID, tt.collateralAsset, tt.collateralDecimals, tt.amount, tt.custodian, tt.state, tt.liquidationLevel, tt.blockNumber, tt.blockVersion)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewMapleCollateral() expected error, got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("NewMapleCollateral() error = %v, want error containing %q", err, tt.errContains)
				}
				if mc != nil {
					t.Error("NewMapleCollateral() expected nil on error")
				}
				return
			}
			if err != nil {
				t.Errorf("NewMapleCollateral() unexpected error = %v", err)
				return
			}
			if mc == nil {
				t.Errorf("NewMapleCollateral() returned nil")
				return
			}
			if mc.ID != 0 {
				t.Errorf("NewMapleCollateral() ID = %d, want 0 (set by DB)", mc.ID)
			}
			if mc.LoanID != tt.loanID {
				t.Errorf("NewMapleCollateral() LoanID = %d, want %d", mc.LoanID, tt.loanID)
			}
			if mc.UserID != tt.userID {
				t.Errorf("NewMapleCollateral() UserID = %d, want %d", mc.UserID, tt.userID)
			}
			if mc.ProtocolID != tt.protocolID {
				t.Errorf("NewMapleCollateral() ProtocolID = %d, want %d", mc.ProtocolID, tt.protocolID)
			}
			if mc.CollateralAsset != tt.collateralAsset {
				t.Errorf("NewMapleCollateral() CollateralAsset = %q, want %q", mc.CollateralAsset, tt.collateralAsset)
			}
			if mc.CollateralDecimals != tt.collateralDecimals {
				t.Errorf("NewMapleCollateral() CollateralDecimals = %d, want %d", mc.CollateralDecimals, tt.collateralDecimals)
			}
			if mc.Amount.Cmp(tt.amount) != 0 {
				t.Errorf("NewMapleCollateral() Amount = %v, want %v", mc.Amount, tt.amount)
			}
			if mc.Custodian != tt.custodian {
				t.Errorf("NewMapleCollateral() Custodian = %q, want %q", mc.Custodian, tt.custodian)
			}
			if mc.State != tt.state {
				t.Errorf("NewMapleCollateral() State = %q, want %q", mc.State, tt.state)
			}
			if tt.liquidationLevel == nil {
				if mc.LiquidationLevel != nil {
					t.Errorf("NewMapleCollateral() LiquidationLevel = %v, want nil", mc.LiquidationLevel)
				}
			} else {
				if mc.LiquidationLevel == nil {
					t.Errorf("NewMapleCollateral() LiquidationLevel = nil, want %v", tt.liquidationLevel)
				} else if mc.LiquidationLevel.Cmp(tt.liquidationLevel) != 0 {
					t.Errorf("NewMapleCollateral() LiquidationLevel = %v, want %v", mc.LiquidationLevel, tt.liquidationLevel)
				}
			}
			if mc.BlockNumber != tt.blockNumber {
				t.Errorf("NewMapleCollateral() BlockNumber = %d, want %d", mc.BlockNumber, tt.blockNumber)
			}
			if mc.BlockVersion != tt.blockVersion {
				t.Errorf("NewMapleCollateral() BlockVersion = %d, want %d", mc.BlockVersion, tt.blockVersion)
			}
		})
	}
}
