package entity

import "testing"

func TestNewToken(t *testing.T) {
	validAddr := make([]byte, 20)
	for i := range validAddr {
		validAddr[i] = byte(i)
	}

	tests := []struct {
		name           string
		id             int64
		chainID        int
		address        []byte
		symbol         string
		decimals       int16
		createdAtBlock int64
		wantErr        bool
		errContains    string
	}{
		{
			name:           "valid token",
			id:             1,
			chainID:        1,
			address:        validAddr,
			symbol:         "DAI",
			decimals:       18,
			createdAtBlock: 1000,
			wantErr:        false,
		},
		{
			name:           "zero id",
			id:             0,
			chainID:        1,
			address:        validAddr,
			symbol:         "DAI",
			decimals:       18,
			createdAtBlock: 1000,
			wantErr:        true,
			errContains:    "id must be positive",
		},
		{
			name:           "negative id",
			id:             -1,
			chainID:        1,
			address:        validAddr,
			symbol:         "DAI",
			decimals:       18,
			createdAtBlock: 1000,
			wantErr:        true,
			errContains:    "id must be positive",
		},
		{
			name:           "zero chainID",
			id:             1,
			chainID:        0,
			address:        validAddr,
			symbol:         "DAI",
			decimals:       18,
			createdAtBlock: 1000,
			wantErr:        true,
			errContains:    "chainID must be positive",
		},
		{
			name:           "invalid address length - too short",
			id:             1,
			chainID:        1,
			address:        make([]byte, 19),
			symbol:         "DAI",
			decimals:       18,
			createdAtBlock: 1000,
			wantErr:        true,
			errContains:    "invalid address length",
		},
		{
			name:           "invalid address length - too long",
			id:             1,
			chainID:        1,
			address:        make([]byte, 21),
			symbol:         "DAI",
			decimals:       18,
			createdAtBlock: 1000,
			wantErr:        true,
			errContains:    "invalid address length",
		},
		{
			name:           "empty symbol",
			id:             1,
			chainID:        1,
			address:        validAddr,
			symbol:         "",
			decimals:       18,
			createdAtBlock: 1000,
			wantErr:        true,
			errContains:    "symbol must not be empty",
		},
		{
			name:           "negative decimals",
			id:             1,
			chainID:        1,
			address:        validAddr,
			symbol:         "DAI",
			decimals:       -1,
			createdAtBlock: 1000,
			wantErr:        true,
			errContains:    "decimals must be non-negative",
		},
		{
			name:           "zero createdAtBlock",
			id:             1,
			chainID:        1,
			address:        validAddr,
			symbol:         "DAI",
			decimals:       18,
			createdAtBlock: 0,
			wantErr:        true,
			errContains:    "createdAtBlock must be positive",
		},
		{
			name:           "valid token with 0 decimals",
			id:             2,
			chainID:        1,
			address:        validAddr,
			symbol:         "NONDEC",
			decimals:       0,
			createdAtBlock: 1000,
			wantErr:        false,
		},
		{
			name:           "valid token with 6 decimals (USDC)",
			id:             3,
			chainID:        1,
			address:        validAddr,
			symbol:         "USDC",
			decimals:       6,
			createdAtBlock: 1000,
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token, err := NewToken(tt.id, tt.chainID, tt.address, tt.symbol, tt.decimals, tt.createdAtBlock)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewToken() expected error, got nil")
					return
				}
				if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("NewToken() error = %v, want error containing %v", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("NewToken() unexpected error = %v", err)
				return
			}
			if token == nil {
				t.Errorf("NewToken() returned nil")
				return
			}
			if token.ID != tt.id {
				t.Errorf("NewToken() ID = %v, want %v", token.ID, tt.id)
			}
			if token.ChainID != tt.chainID {
				t.Errorf("NewToken() ChainID = %v, want %v", token.ChainID, tt.chainID)
			}
			if token.Symbol != tt.symbol {
				t.Errorf("NewToken() Symbol = %v, want %v", token.Symbol, tt.symbol)
			}
			if token.Decimals != tt.decimals {
				t.Errorf("NewToken() Decimals = %v, want %v", token.Decimals, tt.decimals)
			}
			if token.CreatedAtBlock != tt.createdAtBlock {
				t.Errorf("NewToken() CreatedAtBlock = %v, want %v", token.CreatedAtBlock, tt.createdAtBlock)
			}
			if token.Metadata == nil {
				t.Errorf("NewToken() Metadata is nil")
			}
		})
	}
}

func TestToken_AddressHex(t *testing.T) {
	addr := make([]byte, 20)
	for i := range addr {
		addr[i] = byte(i)
	}

	token := &Token{Address: addr}
	hex := token.AddressHex()
	if hex == "" {
		t.Errorf("AddressHex() returned empty string")
	}
	if hex[:2] != "0x" {
		t.Errorf("AddressHex() should start with 0x, got %v", hex)
	}
}
