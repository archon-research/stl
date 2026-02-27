package chainutil

import (
	"strings"
	"testing"
)

func TestValidateS3BucketForChain(t *testing.T) {
	tests := []struct {
		name        string
		chainID     int64
		bucket      string
		wantErr     bool
		errContains string
	}{
		{
			name:    "ethereum mainnet valid bucket",
			chainID: 1,
			bucket:  "stl-sentinelstaging-ethereum-raw",
			wantErr: false,
		},
		{
			name:    "ethereum mainnet with suffix",
			chainID: 1,
			bucket:  "stl-sentinelstaging-ethereum-raw-89d540d0",
			wantErr: false,
		},
		{
			name:    "avalanche valid bucket",
			chainID: 43114,
			bucket:  "stl-sentinelstaging-avalanche-raw",
			wantErr: false,
		},
		{
			name:        "ethereum chain ID with avalanche bucket",
			chainID:     1,
			bucket:      "stl-sentinelstaging-avalanche-raw",
			wantErr:     true,
			errContains: "does not contain expected chain name",
		},
		{
			name:        "avalanche chain ID with ethereum bucket",
			chainID:     43114,
			bucket:      "stl-sentinelstaging-ethereum-raw",
			wantErr:     true,
			errContains: "does not contain expected chain name",
		},
		{
			name:        "unknown chain ID",
			chainID:     999999,
			bucket:      "stl-sentinelstaging-ethereum-raw",
			wantErr:     true,
			errContains: "unknown chain ID",
		},
		{
			name:    "case insensitive match",
			chainID: 1,
			bucket:  "stl-sentinelstaging-ETHEREUM-raw",
			wantErr: false,
		},
		{
			name:        "empty bucket name",
			chainID:     1,
			bucket:      "",
			wantErr:     true,
			errContains: "does not contain expected chain name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateS3BucketForChain(tt.chainID, tt.bucket)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateS3BucketForChain() expected error, got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("ValidateS3BucketForChain() error = %v, want error containing %q", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("ValidateS3BucketForChain() unexpected error = %v", err)
			}
		})
	}
}
