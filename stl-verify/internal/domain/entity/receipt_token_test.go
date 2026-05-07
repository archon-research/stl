package entity

import (
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestNewReceiptToken(t *testing.T) {
	validAddr := common.HexToAddress("0x000102030405060708090a0b0c0d0e0f10111213")

	tests := []struct {
		name                string
		chainID             int64
		protocolID          int64
		underlyingTokenID   int64
		createdAtBlock      int64
		receiptTokenAddress common.Address
		symbol              string
		wantErr             bool
		errContains         string
	}{
		{
			name:                "valid receipt token",
			chainID:             1,
			protocolID:          5,
			underlyingTokenID:   10,
			createdAtBlock:      1000,
			receiptTokenAddress: validAddr,
			symbol:              "spDAI",
			wantErr:             false,
		},
		{
			name:                "zero chainID",
			chainID:             0,
			protocolID:          5,
			underlyingTokenID:   10,
			createdAtBlock:      1000,
			receiptTokenAddress: validAddr,
			symbol:              "spDAI",
			wantErr:             true,
			errContains:         "chainID must be positive",
		},
		{
			name:                "negative chainID",
			chainID:             -1,
			protocolID:          5,
			underlyingTokenID:   10,
			createdAtBlock:      1000,
			receiptTokenAddress: validAddr,
			symbol:              "spDAI",
			wantErr:             true,
			errContains:         "chainID must be positive",
		},
		{
			name:                "zero protocolID",
			chainID:             1,
			protocolID:          0,
			underlyingTokenID:   10,
			createdAtBlock:      1000,
			receiptTokenAddress: validAddr,
			symbol:              "spDAI",
			wantErr:             true,
			errContains:         "protocolID must be positive",
		},
		{
			name:                "zero underlyingTokenID",
			chainID:             1,
			protocolID:          5,
			underlyingTokenID:   0,
			createdAtBlock:      1000,
			receiptTokenAddress: validAddr,
			symbol:              "spDAI",
			wantErr:             true,
			errContains:         "underlyingTokenID must be positive",
		},
		{
			name:                "zero createdAtBlock",
			chainID:             1,
			protocolID:          5,
			underlyingTokenID:   10,
			createdAtBlock:      0,
			receiptTokenAddress: validAddr,
			symbol:              "spDAI",
			wantErr:             true,
			errContains:         "createdAtBlock must be positive",
		},
		{
			name:                "zero address",
			chainID:             1,
			protocolID:          5,
			underlyingTokenID:   10,
			createdAtBlock:      1000,
			receiptTokenAddress: common.Address{},
			symbol:              "spDAI",
			wantErr:             true,
			errContains:         "receipt token address must not be zero",
		},
		{
			name:                "empty symbol is allowed",
			chainID:             1,
			protocolID:          5,
			underlyingTokenID:   10,
			createdAtBlock:      1000,
			receiptTokenAddress: validAddr,
			symbol:              "",
			wantErr:             false,
		},
		{
			name:                "valid aToken",
			chainID:             1,
			protocolID:          5,
			underlyingTokenID:   10,
			createdAtBlock:      5000,
			receiptTokenAddress: validAddr,
			symbol:              "aDAI",
			wantErr:             false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt, err := NewReceiptToken(tt.chainID, tt.protocolID, tt.underlyingTokenID, tt.createdAtBlock, tt.receiptTokenAddress, tt.symbol)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewReceiptToken() expected error, got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("NewReceiptToken() error = %v, want error containing %v", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("NewReceiptToken() unexpected error = %v", err)
				return
			}
			if rt == nil {
				t.Errorf("NewReceiptToken() returned nil")
				return
			}
			if rt.ChainID != tt.chainID {
				t.Errorf("NewReceiptToken() ChainID = %v, want %v", rt.ChainID, tt.chainID)
			}
			if rt.ProtocolID != tt.protocolID {
				t.Errorf("NewReceiptToken() ProtocolID = %v, want %v", rt.ProtocolID, tt.protocolID)
			}
			if rt.UnderlyingTokenID != tt.underlyingTokenID {
				t.Errorf("NewReceiptToken() UnderlyingTokenID = %v, want %v", rt.UnderlyingTokenID, tt.underlyingTokenID)
			}
			if rt.CreatedAtBlock != tt.createdAtBlock {
				t.Errorf("NewReceiptToken() CreatedAtBlock = %v, want %v", rt.CreatedAtBlock, tt.createdAtBlock)
			}
			if rt.Symbol != tt.symbol {
				t.Errorf("NewReceiptToken() Symbol = %v, want %v", rt.Symbol, tt.symbol)
			}
			if rt.Metadata == nil {
				t.Errorf("NewReceiptToken() Metadata is nil")
			}
		})
	}
}

func TestReceiptToken_AddressHex(t *testing.T) {
	addr := common.HexToAddress("0x000102030405060708090a0b0c0d0e0f10111213")

	rt := &ReceiptToken{ReceiptTokenAddress: addr}
	hex := rt.AddressHex()
	if hex == "" {
		t.Errorf("AddressHex() returned empty string")
	}
	if hex[:2] != "0x" {
		t.Errorf("AddressHex() should start with 0x, got %v", hex)
	}
	// Verify EIP-55 checksummed output
	expected := common.HexToAddress("0x000102030405060708090a0b0c0d0e0f10111213").Hex()
	if hex != expected {
		t.Errorf("AddressHex() = %v, want %v (EIP-55 checksummed)", hex, expected)
	}
}
