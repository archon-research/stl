package entity

import "testing"

func TestNewChain(t *testing.T) {
	tests := []struct {
		name        string
		chainID     int
		chainName   string
		wantErr     bool
		errContains string
	}{
		{
			name:      "valid chain",
			chainID:   1,
			chainName: "mainnet",
			wantErr:   false,
		},
		{
			name:        "zero chainID",
			chainID:     0,
			chainName:   "mainnet",
			wantErr:     true,
			errContains: "chainID must be positive",
		},
		{
			name:        "negative chainID",
			chainID:     -1,
			chainName:   "mainnet",
			wantErr:     true,
			errContains: "chainID must be positive",
		},
		{
			name:        "empty name",
			chainID:     1,
			chainName:   "",
			wantErr:     true,
			errContains: "name must not be empty",
		},
		{
			name:      "valid arbitrum chain",
			chainID:   42161,
			chainName: "arbitrum",
			wantErr:   false,
		},
		{
			name:      "valid polygon chain",
			chainID:   137,
			chainName: "polygon",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chain, err := NewChain(tt.chainID, tt.chainName)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewChain() expected error, got nil")
					return
				}
				if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("NewChain() error = %v, want error containing %v", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("NewChain() unexpected error = %v", err)
				return
			}
			if chain == nil {
				t.Errorf("NewChain() returned nil")
				return
			}
			if chain.ChainID != tt.chainID {
				t.Errorf("NewChain() ChainID = %v, want %v", chain.ChainID, tt.chainID)
			}
			if chain.Name != tt.chainName {
				t.Errorf("NewChain() Name = %v, want %v", chain.Name, tt.chainName)
			}
		})
	}
}
