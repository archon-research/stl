package entity

import "testing"

func TestNewDebtToken(t *testing.T) {
	validVariableAddr := make([]byte, 20)
	validStableAddr := make([]byte, 20)
	for i := range validVariableAddr {
		validVariableAddr[i] = byte(i)
	}
	for i := range validStableAddr {
		validStableAddr[i] = byte(i + 20)
	}

	tests := []struct {
		name                string
		id                  int64
		protocolID          int64
		underlyingTokenID   int64
		createdAtBlock      int64
		variableDebtAddress []byte
		stableDebtAddress   []byte
		variableSymbol      string
		stableSymbol        string
		wantErr             bool
		errContains         string
	}{
		{
			name:                "valid debt token with both addresses",
			id:                  1,
			protocolID:          5,
			underlyingTokenID:   10,
			createdAtBlock:      1000,
			variableDebtAddress: validVariableAddr,
			stableDebtAddress:   validStableAddr,
			variableSymbol:      "variableDebtDAI",
			stableSymbol:        "stableDebtDAI",
			wantErr:             false,
		},
		{
			name:                "valid with only variable address",
			id:                  1,
			protocolID:          5,
			underlyingTokenID:   10,
			createdAtBlock:      1000,
			variableDebtAddress: validVariableAddr,
			stableDebtAddress:   nil,
			variableSymbol:      "variableDebtDAI",
			stableSymbol:        "",
			wantErr:             false,
		},
		{
			name:                "valid with only stable address",
			id:                  1,
			protocolID:          5,
			underlyingTokenID:   10,
			createdAtBlock:      1000,
			variableDebtAddress: nil,
			stableDebtAddress:   validStableAddr,
			variableSymbol:      "",
			stableSymbol:        "stableDebtDAI",
			wantErr:             false,
		},
		{
			name:                "zero id",
			id:                  0,
			protocolID:          5,
			underlyingTokenID:   10,
			createdAtBlock:      1000,
			variableDebtAddress: validVariableAddr,
			stableDebtAddress:   validStableAddr,
			variableSymbol:      "variableDebtDAI",
			stableSymbol:        "stableDebtDAI",
			wantErr:             true,
			errContains:         "id must be positive",
		},
		{
			name:                "zero protocolID",
			id:                  1,
			protocolID:          0,
			underlyingTokenID:   10,
			createdAtBlock:      1000,
			variableDebtAddress: validVariableAddr,
			stableDebtAddress:   validStableAddr,
			variableSymbol:      "variableDebtDAI",
			stableSymbol:        "stableDebtDAI",
			wantErr:             true,
			errContains:         "protocolID must be positive",
		},
		{
			name:                "zero underlyingTokenID",
			id:                  1,
			protocolID:          5,
			underlyingTokenID:   0,
			createdAtBlock:      1000,
			variableDebtAddress: validVariableAddr,
			stableDebtAddress:   validStableAddr,
			variableSymbol:      "variableDebtDAI",
			stableSymbol:        "stableDebtDAI",
			wantErr:             true,
			errContains:         "underlyingTokenID must be positive",
		},
		{
			name:                "zero createdAtBlock",
			id:                  1,
			protocolID:          5,
			underlyingTokenID:   10,
			createdAtBlock:      0,
			variableDebtAddress: validVariableAddr,
			stableDebtAddress:   validStableAddr,
			variableSymbol:      "variableDebtDAI",
			stableSymbol:        "stableDebtDAI",
			wantErr:             true,
			errContains:         "createdAtBlock must be positive",
		},
		{
			name:                "invalid variable address length",
			id:                  1,
			protocolID:          5,
			underlyingTokenID:   10,
			createdAtBlock:      1000,
			variableDebtAddress: make([]byte, 19),
			stableDebtAddress:   validStableAddr,
			variableSymbol:      "variableDebtDAI",
			stableSymbol:        "stableDebtDAI",
			wantErr:             true,
			errContains:         "invalid variable debt address length",
		},
		{
			name:                "invalid stable address length",
			id:                  1,
			protocolID:          5,
			underlyingTokenID:   10,
			createdAtBlock:      1000,
			variableDebtAddress: validVariableAddr,
			stableDebtAddress:   make([]byte, 21),
			variableSymbol:      "variableDebtDAI",
			stableSymbol:        "stableDebtDAI",
			wantErr:             true,
			errContains:         "invalid stable debt address length",
		},
		{
			name:                "both addresses nil",
			id:                  1,
			protocolID:          5,
			underlyingTokenID:   10,
			createdAtBlock:      1000,
			variableDebtAddress: nil,
			stableDebtAddress:   nil,
			variableSymbol:      "variableDebtDAI",
			stableSymbol:        "stableDebtDAI",
			wantErr:             true,
			errContains:         "at least one debt address must be provided",
		},
		{
			name:                "both symbols empty",
			id:                  1,
			protocolID:          5,
			underlyingTokenID:   10,
			createdAtBlock:      1000,
			variableDebtAddress: validVariableAddr,
			stableDebtAddress:   validStableAddr,
			variableSymbol:      "",
			stableSymbol:        "",
			wantErr:             true,
			errContains:         "at least one symbol must be provided",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt, err := NewDebtToken(tt.id, tt.protocolID, tt.underlyingTokenID, tt.createdAtBlock, tt.variableDebtAddress, tt.stableDebtAddress, tt.variableSymbol, tt.stableSymbol)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewDebtToken() expected error, got nil")
					return
				}
				if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("NewDebtToken() error = %v, want error containing %v", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("NewDebtToken() unexpected error = %v", err)
				return
			}
			if dt == nil {
				t.Errorf("NewDebtToken() returned nil")
				return
			}
			if dt.ID != tt.id {
				t.Errorf("NewDebtToken() ID = %v, want %v", dt.ID, tt.id)
			}
			if dt.ProtocolID != tt.protocolID {
				t.Errorf("NewDebtToken() ProtocolID = %v, want %v", dt.ProtocolID, tt.protocolID)
			}
			if dt.UnderlyingTokenID != tt.underlyingTokenID {
				t.Errorf("NewDebtToken() UnderlyingTokenID = %v, want %v", dt.UnderlyingTokenID, tt.underlyingTokenID)
			}
			if dt.CreatedAtBlock != tt.createdAtBlock {
				t.Errorf("NewDebtToken() CreatedAtBlock = %v, want %v", dt.CreatedAtBlock, tt.createdAtBlock)
			}
			if dt.VariableSymbol != tt.variableSymbol {
				t.Errorf("NewDebtToken() VariableSymbol = %v, want %v", dt.VariableSymbol, tt.variableSymbol)
			}
			if dt.StableSymbol != tt.stableSymbol {
				t.Errorf("NewDebtToken() StableSymbol = %v, want %v", dt.StableSymbol, tt.stableSymbol)
			}
			if dt.Metadata == nil {
				t.Errorf("NewDebtToken() Metadata is nil")
			}
		})
	}
}

func TestDebtToken_VariableAddressHex(t *testing.T) {
	addr := make([]byte, 20)
	for i := range addr {
		addr[i] = byte(i)
	}

	dt := &DebtToken{VariableDebtAddress: addr}
	hex := dt.VariableAddressHex()
	if hex == "" {
		t.Errorf("VariableAddressHex() returned empty string")
	}
	if hex[:2] != "0x" {
		t.Errorf("VariableAddressHex() should start with 0x, got %v", hex)
	}

	dtNil := &DebtToken{VariableDebtAddress: nil}
	if dtNil.VariableAddressHex() != "" {
		t.Errorf("VariableAddressHex() with nil address should return empty string")
	}
}

func TestDebtToken_StableAddressHex(t *testing.T) {
	addr := make([]byte, 20)
	for i := range addr {
		addr[i] = byte(i)
	}

	dt := &DebtToken{StableDebtAddress: addr}
	hex := dt.StableAddressHex()
	if hex == "" {
		t.Errorf("StableAddressHex() returned empty string")
	}
	if hex[:2] != "0x" {
		t.Errorf("StableAddressHex() should start with 0x, got %v", hex)
	}

	dtNil := &DebtToken{StableDebtAddress: nil}
	if dtNil.StableAddressHex() != "" {
		t.Errorf("StableAddressHex() with nil address should return empty string")
	}
}
