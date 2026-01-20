package entity

import (
	"strings"
	"testing"
)

func TestNewReceiptToken(t *testing.T) {
	validAddr := make([]byte, 20)
	for i := range validAddr {
		validAddr[i] = byte(i)
	}

	tests := []struct {
		name                string
		id                  int64
		protocolID          int64
		underlyingTokenID   int64
		createdAtBlock      int64
		receiptTokenAddress []byte
		symbol              string
		wantErr             bool
		errContains         string
	}{
		{
			name:                "valid receipt token",
			id:                  1,
			protocolID:          5,
			underlyingTokenID:   10,
			createdAtBlock:      1000,
			receiptTokenAddress: validAddr,
			symbol:              "spDAI",
			wantErr:             false,
		},
		{
			name:                "zero id",
			id:                  0,
			protocolID:          5,
			underlyingTokenID:   10,
			createdAtBlock:      1000,
			receiptTokenAddress: validAddr,
			symbol:              "spDAI",
			wantErr:             true,
			errContains:         "id must be positive",
		},
		{
			name:                "negative id",
			id:                  -1,
			protocolID:          5,
			underlyingTokenID:   10,
			createdAtBlock:      1000,
			receiptTokenAddress: validAddr,
			symbol:              "spDAI",
			wantErr:             true,
			errContains:         "id must be positive",
		},
		{
			name:                "zero protocolID",
			id:                  1,
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
			id:                  1,
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
			id:                  1,
			protocolID:          5,
			underlyingTokenID:   10,
			createdAtBlock:      0,
			receiptTokenAddress: validAddr,
			symbol:              "spDAI",
			wantErr:             true,
			errContains:         "createdAtBlock must be positive",
		},
		{
			name:                "invalid address length - too short",
			id:                  1,
			protocolID:          5,
			underlyingTokenID:   10,
			createdAtBlock:      1000,
			receiptTokenAddress: make([]byte, 19),
			symbol:              "spDAI",
			wantErr:             true,
			errContains:         "invalid receipt token address length",
		},
		{
			name:                "invalid address length - too long",
			id:                  1,
			protocolID:          5,
			underlyingTokenID:   10,
			createdAtBlock:      1000,
			receiptTokenAddress: make([]byte, 21),
			symbol:              "spDAI",
			wantErr:             true,
			errContains:         "invalid receipt token address length",
		},
		{
			name:                "empty symbol",
			id:                  1,
			protocolID:          5,
			underlyingTokenID:   10,
			createdAtBlock:      1000,
			receiptTokenAddress: validAddr,
			symbol:              "",
			wantErr:             true,
			errContains:         "symbol must not be empty",
		},
		{
			name:                "valid aToken",
			id:                  2,
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
			rt, err := NewReceiptToken(tt.id, tt.protocolID, tt.underlyingTokenID, tt.createdAtBlock, tt.receiptTokenAddress, tt.symbol)
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
			if rt.ID != tt.id {
				t.Errorf("NewReceiptToken() ID = %v, want %v", rt.ID, tt.id)
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
	addr := make([]byte, 20)
	for i := range addr {
		addr[i] = byte(i)
	}

	rt := &ReceiptToken{ReceiptTokenAddress: addr}
	hex := rt.AddressHex()
	if hex == "" {
		t.Errorf("AddressHex() returned empty string")
	}
	if hex[:2] != "0x" {
		t.Errorf("AddressHex() should start with 0x, got %v", hex)
	}
}
