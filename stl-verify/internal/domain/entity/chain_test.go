package entity

import (
	"strings"
	"testing"
)

func TestChainName(t *testing.T) {
	tests := []struct {
		name     string
		chainID  int64
		want     string
		wantErr  bool
		errMatch string
	}{
		{name: "mainnet", chainID: 1, want: "mainnet"},
		{name: "arbitrum", chainID: 42161, want: "arbitrum"},
		{name: "avalanche", chainID: 43114, want: "avalanche-c"},
		{name: "unknown", chainID: 999999, wantErr: true, errMatch: "unknown chain ID 999999"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ChainName(tt.chainID)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ChainName(%d) expected error, got nil", tt.chainID)
				}
				if tt.errMatch != "" && !strings.Contains(err.Error(), tt.errMatch) {
					t.Errorf("ChainName(%d) error = %q, want containing %q", tt.chainID, err, tt.errMatch)
				}
				return
			}
			if err != nil {
				t.Fatalf("ChainName(%d) unexpected error: %v", tt.chainID, err)
			}
			if got != tt.want {
				t.Errorf("ChainName(%d) = %q, want %q", tt.chainID, got, tt.want)
			}
		})
	}
}

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
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
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
