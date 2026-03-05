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
		environment string
		wantErr     bool
		errContains string
	}{
		{
			name:        "ethereum mainnet valid bucket",
			chainID:     1,
			bucket:      "stl-sentinelstaging-ethereum-raw",
			environment: "staging",
			wantErr:     false,
		},
		{
			name:        "ethereum mainnet with suffix",
			chainID:     1,
			bucket:      "stl-sentinelstaging-ethereum-raw-89d540d0",
			environment: "staging",
			wantErr:     false,
		},
		{
			name:        "avalanche valid bucket",
			chainID:     43114,
			bucket:      "stl-sentinelstaging-avalanche-raw",
			environment: "staging",
			wantErr:     false,
		},
		{
			name:        "ethereum chain ID with avalanche bucket",
			chainID:     1,
			bucket:      "stl-sentinelstaging-avalanche-raw",
			environment: "staging",
			wantErr:     true,
			errContains: "does not have expected prefix",
		},
		{
			name:        "avalanche chain ID with ethereum bucket",
			chainID:     43114,
			bucket:      "stl-sentinelstaging-ethereum-raw",
			environment: "staging",
			wantErr:     true,
			errContains: "does not have expected prefix",
		},
		{
			name:        "unknown chain ID",
			chainID:     999999,
			bucket:      "stl-sentinelstaging-ethereum-raw",
			environment: "staging",
			wantErr:     true,
			errContains: "unknown chain ID",
		},
		{
			name:        "case insensitive match",
			chainID:     1,
			bucket:      "stl-sentinelstaging-ETHEREUM-raw",
			environment: "staging",
			wantErr:     false,
		},
		{
			name:        "empty bucket name",
			chainID:     1,
			bucket:      "",
			environment: "staging",
			wantErr:     true,
			errContains: "does not have expected prefix",
		},
		{
			name:        "substring-only chain token should fail",
			chainID:     1,
			bucket:      "stl-sentinelstaging-notethereumish-raw",
			environment: "staging",
			wantErr:     true,
			errContains: "does not have expected prefix",
		},
		{
			name:        "empty environment",
			chainID:     1,
			bucket:      "stl-sentinelstaging-ethereum-raw",
			environment: "",
			wantErr:     true,
			errContains: "environment must not be empty",
		},
		{
			name:        "prod environment",
			chainID:     1,
			bucket:      "stl-sentinelprod-ethereum-raw",
			environment: "prod",
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateS3BucketForChain(tt.chainID, tt.bucket, tt.environment)
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
