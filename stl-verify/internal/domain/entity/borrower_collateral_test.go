package entity

import (
	"bytes"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

func TestNewBorrowerCollateral(t *testing.T) {
	validAmount := big.NewInt(1000)
	validChange := big.NewInt(100)
	validEventType := EventSupply
	validTxHash := common.FromHex("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")

	testCreatedAt := time.Unix(1700000000, 0).UTC()

	tests := []struct {
		name              string
		userID            int64
		protocolID        int64
		tokenID           int64
		blockNumber       int64
		blockVersion      int
		amount            *big.Int
		change            *big.Int
		eventType         EventType
		txHash            []byte
		collateralEnabled bool
		wantErr           bool
		errContains       string
	}{
		{
			name:              "valid borrower collateral",
			userID:            10,
			protocolID:        5,
			tokenID:           3,
			blockNumber:       1000,
			blockVersion:      0,
			amount:            validAmount,
			change:            validChange,
			eventType:         validEventType,
			txHash:            validTxHash,
			collateralEnabled: true,
			wantErr:           false,
		},
		{
			name:              "valid borrower collateral with collateral disabled",
			userID:            10,
			protocolID:        5,
			tokenID:           3,
			blockNumber:       1000,
			blockVersion:      0,
			amount:            validAmount,
			change:            validChange,
			eventType:         EventWithdraw,
			txHash:            validTxHash,
			collateralEnabled: false,
			wantErr:           false,
		},
		{
			name:              "zero userID",
			userID:            0,
			protocolID:        5,
			tokenID:           3,
			blockNumber:       1000,
			blockVersion:      0,
			amount:            validAmount,
			change:            validChange,
			eventType:         validEventType,
			txHash:            validTxHash,
			collateralEnabled: true,
			wantErr:           true,
			errContains:       "userID must be positive",
		},
		{
			name: "zero protocolID",

			userID:            10,
			protocolID:        0,
			tokenID:           3,
			blockNumber:       1000,
			blockVersion:      0,
			amount:            validAmount,
			change:            validChange,
			eventType:         validEventType,
			txHash:            validTxHash,
			collateralEnabled: true,
			wantErr:           true,
			errContains:       "protocolID must be positive",
		},
		{
			name: "zero tokenID",

			userID:            10,
			protocolID:        5,
			tokenID:           0,
			blockNumber:       1000,
			blockVersion:      0,
			amount:            validAmount,
			change:            validChange,
			eventType:         validEventType,
			txHash:            validTxHash,
			collateralEnabled: true,
			wantErr:           true,
			errContains:       "tokenID must be positive",
		},
		{
			name: "zero blockNumber",

			userID:            10,
			protocolID:        5,
			tokenID:           3,
			blockNumber:       0,
			blockVersion:      0,
			amount:            validAmount,
			change:            validChange,
			eventType:         validEventType,
			txHash:            validTxHash,
			collateralEnabled: true,
			wantErr:           true,
			errContains:       "blockNumber must be positive",
		},
		{
			name: "negative blockVersion",

			userID:            10,
			protocolID:        5,
			tokenID:           3,
			blockNumber:       1000,
			blockVersion:      -1,
			amount:            validAmount,
			change:            validChange,
			eventType:         validEventType,
			txHash:            validTxHash,
			collateralEnabled: true,
			wantErr:           true,
			errContains:       "blockVersion must be non-negative",
		},
		{
			name: "nil amount",

			userID:            10,
			protocolID:        5,
			tokenID:           3,
			blockNumber:       1000,
			blockVersion:      0,
			amount:            nil,
			change:            validChange,
			eventType:         validEventType,
			txHash:            validTxHash,
			collateralEnabled: true,
			wantErr:           true,
			errContains:       "amount must not be nil",
		},
		{
			name: "negative amount",

			userID:            10,
			protocolID:        5,
			tokenID:           3,
			blockNumber:       1000,
			blockVersion:      0,
			amount:            big.NewInt(-100),
			change:            validChange,
			eventType:         validEventType,
			txHash:            validTxHash,
			collateralEnabled: true,
			wantErr:           true,
			errContains:       "amount must be non-negative",
		},
		{
			name: "nil change",

			userID:            10,
			protocolID:        5,
			tokenID:           3,
			blockNumber:       1000,
			blockVersion:      0,
			amount:            validAmount,
			change:            nil,
			eventType:         validEventType,
			txHash:            validTxHash,
			collateralEnabled: true,
			wantErr:           true,
			errContains:       "change must not be nil",
		},
		{
			name: "zero amount and change",

			userID:            10,
			protocolID:        5,
			tokenID:           3,
			blockNumber:       1000,
			blockVersion:      0,
			amount:            big.NewInt(0),
			change:            big.NewInt(0),
			eventType:         validEventType,
			txHash:            validTxHash,
			collateralEnabled: true,
			wantErr:           false,
		},
		{
			name: "negative change allowed",

			userID:            10,
			protocolID:        5,
			tokenID:           3,
			blockNumber:       1000,
			blockVersion:      0,
			amount:            validAmount,
			change:            big.NewInt(-50),
			eventType:         validEventType,
			txHash:            validTxHash,
			collateralEnabled: true,
			wantErr:           false,
		},
		{
			name: "empty eventType",

			userID:            10,
			protocolID:        5,
			tokenID:           3,
			blockNumber:       1000,
			blockVersion:      0,
			amount:            validAmount,
			change:            validChange,
			eventType:         "",
			txHash:            validTxHash,
			collateralEnabled: true,
			wantErr:           true,
			errContains:       "invalid eventType",
		},
		{
			name: "invalid eventType",

			userID:            10,
			protocolID:        5,
			tokenID:           3,
			blockNumber:       1000,
			blockVersion:      0,
			amount:            validAmount,
			change:            validChange,
			eventType:         "InvalidEvent",
			txHash:            validTxHash,
			collateralEnabled: true,
			wantErr:           true,
			errContains:       "invalid eventType",
		},
		{
			name: "empty txHash",

			userID:            10,
			protocolID:        5,
			tokenID:           3,
			blockNumber:       1000,
			blockVersion:      0,
			amount:            validAmount,
			change:            validChange,
			eventType:         validEventType,
			txHash:            nil,
			collateralEnabled: true,
			wantErr:           true,
			errContains:       "txHash must not be empty",
		},
		{
			name: "liquidation event type",

			userID:            10,
			protocolID:        5,
			tokenID:           3,
			blockNumber:       1000,
			blockVersion:      0,
			amount:            validAmount,
			change:            validChange,
			eventType:         EventLiquidationCall,
			txHash:            validTxHash,
			collateralEnabled: true,
			wantErr:           false,
		},
		{
			name: "reserve collateral enabled event type",

			userID:            10,
			protocolID:        5,
			tokenID:           3,
			blockNumber:       1000,
			blockVersion:      0,
			amount:            validAmount,
			change:            validChange,
			eventType:         EventReserveUsedAsCollateralEnabled,
			txHash:            validTxHash,
			collateralEnabled: true,
			wantErr:           false,
		},
		{
			name: "reserve collateral disabled event type",

			userID:            10,
			protocolID:        5,
			tokenID:           3,
			blockNumber:       1000,
			blockVersion:      0,
			amount:            validAmount,
			change:            validChange,
			eventType:         EventReserveUsedAsCollateralDisabled,
			txHash:            validTxHash,
			collateralEnabled: false,
			wantErr:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bc, err := NewBorrowerCollateral(tt.userID, tt.protocolID, tt.tokenID, tt.blockNumber, tt.blockVersion, tt.amount, tt.change, tt.eventType, tt.txHash, tt.collateralEnabled, testCreatedAt)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewBorrowerCollateral() expected error, got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("NewBorrowerCollateral() error = %v, want error containing %v", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("NewBorrowerCollateral() unexpected error = %v", err)
				return
			}
			if bc == nil {
				t.Errorf("NewBorrowerCollateral() returned nil")
				return
			}
			if bc.UserID != tt.userID {
				t.Errorf("NewBorrowerCollateral() UserID = %v, want %v", bc.UserID, tt.userID)
			}
			if bc.ProtocolID != tt.protocolID {
				t.Errorf("NewBorrowerCollateral() ProtocolID = %v, want %v", bc.ProtocolID, tt.protocolID)
			}
			if bc.TokenID != tt.tokenID {
				t.Errorf("NewBorrowerCollateral() TokenID = %v, want %v", bc.TokenID, tt.tokenID)
			}
			if bc.BlockNumber != tt.blockNumber {
				t.Errorf("NewBorrowerCollateral() BlockNumber = %v, want %v", bc.BlockNumber, tt.blockNumber)
			}
			if bc.BlockVersion != tt.blockVersion {
				t.Errorf("NewBorrowerCollateral() BlockVersion = %v, want %v", bc.BlockVersion, tt.blockVersion)
			}
			if bc.Amount.Cmp(tt.amount) != 0 {
				t.Errorf("NewBorrowerCollateral() Amount = %v, want %v", bc.Amount, tt.amount)
			}
			if bc.Change.Cmp(tt.change) != 0 {
				t.Errorf("NewBorrowerCollateral() Change = %v, want %v", bc.Change, tt.change)
			}
			if bc.EventType != tt.eventType {
				t.Errorf("NewBorrowerCollateral() EventType = %v, want %v", bc.EventType, tt.eventType)
			}
			if !bytes.Equal(bc.TxHash, tt.txHash) {
				t.Errorf("NewBorrowerCollateral() TxHash = %v, want %v", bc.TxHash, tt.txHash)
			}
			if bc.CollateralEnabled != tt.collateralEnabled {
				t.Errorf("NewBorrowerCollateral() CollateralEnabled = %v, want %v", bc.CollateralEnabled, tt.collateralEnabled)
			}
		})
	}
}
