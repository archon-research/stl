package entity

import (
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestNewMorphoAdapterState(t *testing.T) {
	ts := time.Unix(1700000000, 0).UTC()

	tests := []struct {
		name        string
		adapterID   int64
		block       int64
		version     int
		timestamp   time.Time
		realAssets  *big.Int
		wantErr     bool
		errContains string
	}{
		{
			name: "valid", adapterID: 1, block: 24481834, version: 0, timestamp: ts,
			realAssets: big.NewInt(1_000_000),
		},
		{
			name: "valid zero real assets", adapterID: 1, block: 24481834, version: 2, timestamp: ts,
			realAssets: big.NewInt(0),
		},
		{
			name: "zero adapter id", adapterID: 0, block: 1, version: 0, timestamp: ts,
			realAssets: big.NewInt(1),
			wantErr:    true, errContains: "morphoAdapterID must be positive",
		},
		{
			name: "zero block", adapterID: 1, block: 0, version: 0, timestamp: ts,
			realAssets: big.NewInt(1),
			wantErr:    true, errContains: "blockNumber must be positive",
		},
		{
			name: "negative block version", adapterID: 1, block: 1, version: -1, timestamp: ts,
			realAssets: big.NewInt(1),
			wantErr:    true, errContains: "blockVersion must be non-negative",
		},
		{
			name: "zero timestamp", adapterID: 1, block: 1, version: 0, timestamp: time.Time{},
			realAssets: big.NewInt(1),
			wantErr:    true, errContains: "timestamp must not be zero",
		},
		{
			name: "nil real assets", adapterID: 1, block: 1, version: 0, timestamp: ts,
			realAssets: nil,
			wantErr:    true, errContains: "realAssets must not be nil",
		},
		{
			name: "negative real assets", adapterID: 1, block: 1, version: 0, timestamp: ts,
			realAssets: big.NewInt(-1),
			wantErr:    true, errContains: "realAssets must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMorphoAdapterState(tt.adapterID, tt.block, tt.version, tt.timestamp, tt.realAssets)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.RealAssets.Cmp(tt.realAssets) != 0 {
				t.Errorf("RealAssets = %s, want %s", got.RealAssets, tt.realAssets)
			}
		})
	}
}
