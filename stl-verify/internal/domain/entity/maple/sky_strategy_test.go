package maple

import (
	"bytes"
	"strings"
	"testing"
)

func validSkyStrategy() *SkyStrategy {
	return &SkyStrategy{
		ChainID:         1,
		StrategyAddress: bytes.Repeat([]byte{0xdd}, 20),
		PoolID:          3,
		Version:         100,
	}
}

func TestSkyStrategy_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(s *SkyStrategy)
		wantErr string
	}{
		{name: "valid strategy"},
		{name: "zero version ok", mutate: func(s *SkyStrategy) { s.Version = 0 }},
		{
			name:    "zero chain ID",
			mutate:  func(s *SkyStrategy) { s.ChainID = 0 },
			wantErr: "chainID must be positive",
		},
		{
			name:    "short strategy address",
			mutate:  func(s *SkyStrategy) { s.StrategyAddress = []byte{0x01} },
			wantErr: "strategyAddress must be 20 bytes",
		},
		{
			name:    "nil strategy address",
			mutate:  func(s *SkyStrategy) { s.StrategyAddress = nil },
			wantErr: "strategyAddress must be 20 bytes",
		},
		{
			name:    "zero pool ID",
			mutate:  func(s *SkyStrategy) { s.PoolID = 0 },
			wantErr: "maplePoolID must be positive",
		},
		{
			name:    "negative version",
			mutate:  func(s *SkyStrategy) { s.Version = -1 },
			wantErr: "version must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := validSkyStrategy()
			if tt.mutate != nil {
				tt.mutate(s)
			}
			err := s.Validate()
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

func TestNewSkyStrategy_Constructor(t *testing.T) {
	v := validSkyStrategy()

	got, err := NewSkyStrategy(v.ChainID, v.StrategyAddress, v.PoolID, v.Version)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Version != v.Version || got.PoolID != v.PoolID {
		t.Errorf("fields not set: %+v", got)
	}

	if _, err := NewSkyStrategy(0, v.StrategyAddress, v.PoolID, v.Version); err == nil {
		t.Fatal("expected constructor to propagate validation error")
	} else if !strings.Contains(err.Error(), "NewSkyStrategy") {
		t.Errorf("error %q should be wrapped with constructor name", err.Error())
	}
}
