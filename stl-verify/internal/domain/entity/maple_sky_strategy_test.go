package entity

import (
	"bytes"
	"strings"
	"testing"
)

func validMapleSkyStrategy() *MapleSkyStrategy {
	return &MapleSkyStrategy{
		ChainID:         1,
		StrategyAddress: bytes.Repeat([]byte{0xdd}, 20),
		MaplePoolID:     3,
		Version:         100,
	}
}

func TestMapleSkyStrategy_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(s *MapleSkyStrategy)
		wantErr string
	}{
		{name: "valid strategy"},
		{name: "zero version ok", mutate: func(s *MapleSkyStrategy) { s.Version = 0 }},
		{
			name:    "zero chain ID",
			mutate:  func(s *MapleSkyStrategy) { s.ChainID = 0 },
			wantErr: "chainID must be positive",
		},
		{
			name:    "short strategy address",
			mutate:  func(s *MapleSkyStrategy) { s.StrategyAddress = []byte{0x01} },
			wantErr: "strategyAddress must be 20 bytes",
		},
		{
			name:    "nil strategy address",
			mutate:  func(s *MapleSkyStrategy) { s.StrategyAddress = nil },
			wantErr: "strategyAddress must be 20 bytes",
		},
		{
			name:    "zero pool ID",
			mutate:  func(s *MapleSkyStrategy) { s.MaplePoolID = 0 },
			wantErr: "maplePoolID must be positive",
		},
		{
			name:    "negative version",
			mutate:  func(s *MapleSkyStrategy) { s.Version = -1 },
			wantErr: "version must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := validMapleSkyStrategy()
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

func TestNewMapleSkyStrategy_Constructor(t *testing.T) {
	v := validMapleSkyStrategy()

	got, err := NewMapleSkyStrategy(v.ChainID, v.StrategyAddress, v.MaplePoolID, v.Version)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Version != v.Version || got.MaplePoolID != v.MaplePoolID {
		t.Errorf("fields not set: %+v", got)
	}

	if _, err := NewMapleSkyStrategy(0, v.StrategyAddress, v.MaplePoolID, v.Version); err == nil {
		t.Fatal("expected constructor to propagate validation error")
	} else if !strings.Contains(err.Error(), "NewMapleSkyStrategy") {
		t.Errorf("error %q should be wrapped with constructor name", err.Error())
	}
}
