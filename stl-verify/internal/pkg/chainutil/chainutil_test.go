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

func TestValidateSNSTopicForChain(t *testing.T) {
	tests := []struct {
		name        string
		chainID     int64
		topicARN    string
		environment string
		wantErr     bool
		errContains string
	}{
		{
			name:        "ethereum staging valid",
			chainID:     1,
			topicARN:    "arn:aws:sns:eu-west-1:123456789012:stl-sentinelstaging-ethereum-blocks.fifo",
			environment: "staging",
		},
		{
			name:        "avalanche staging valid",
			chainID:     43114,
			topicARN:    "arn:aws:sns:eu-west-1:123456789012:stl-sentinelstaging-avalanche-blocks.fifo",
			environment: "staging",
		},
		{
			name:        "prod environment valid",
			chainID:     1,
			topicARN:    "arn:aws:sns:eu-west-1:123456789012:stl-sentinelprod-ethereum-blocks.fifo",
			environment: "prod",
		},
		{
			name:        "ethereum chain ID with avalanche topic — must error",
			chainID:     1,
			topicARN:    "arn:aws:sns:eu-west-1:123456789012:stl-sentinelstaging-avalanche-blocks.fifo",
			environment: "staging",
			wantErr:     true,
			errContains: "does not have expected suffix",
		},
		{
			name:        "staging chain with prod topic — must error",
			chainID:     1,
			topicARN:    "arn:aws:sns:eu-west-1:123456789012:stl-sentinelprod-ethereum-blocks.fifo",
			environment: "staging",
			wantErr:     true,
			errContains: "does not have expected suffix",
		},
		{
			name:        "unknown chain ID",
			chainID:     999999,
			topicARN:    "arn:aws:sns:eu-west-1:123456789012:stl-sentinelstaging-ethereum-blocks.fifo",
			environment: "staging",
			wantErr:     true,
			errContains: "unknown chain ID",
		},
		{
			name:        "empty environment",
			chainID:     1,
			topicARN:    "arn:aws:sns:eu-west-1:123456789012:stl-sentinelstaging-ethereum-blocks.fifo",
			environment: "",
			wantErr:     true,
			errContains: "environment must not be empty",
		},
		{
			name:        "case-insensitive ARN match",
			chainID:     1,
			topicARN:    "ARN:AWS:SNS:EU-WEST-1:123456789012:STL-SENTINELSTAGING-ETHEREUM-BLOCKS.FIFO",
			environment: "staging",
		},
		{
			name:        "non-FIFO topic — must error",
			chainID:     1,
			topicARN:    "arn:aws:sns:eu-west-1:123456789012:stl-sentinelstaging-ethereum-blocks",
			environment: "staging",
			wantErr:     true,
			errContains: "does not have expected suffix",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSNSTopicForChain(tt.chainID, tt.topicARN, tt.environment)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateSNSTopicForChain() expected error, got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("ValidateSNSTopicForChain() error = %v, want error containing %q", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("ValidateSNSTopicForChain() unexpected error = %v", err)
			}
		})
	}
}

func TestEnvironmentFromBucket(t *testing.T) {
	tests := []struct {
		name        string
		bucket      string
		wantEnv     string
		wantErr     bool
		errContains string
	}{
		{name: "staging bare", bucket: "stl-sentinelstaging-ethereum-raw", wantEnv: "staging"},
		{name: "staging with suffix", bucket: "stl-sentinelstaging-ethereum-raw-89d540d0", wantEnv: "staging"},
		{name: "prod", bucket: "stl-sentinelprod-avalanche-raw", wantEnv: "prod"},
		{name: "case insensitive", bucket: "STL-SENTINELSTAGING-ethereum-raw", wantEnv: "staging"},
		{name: "wrong prefix", bucket: "my-test-bucket", wantErr: true, errContains: "does not start with"},
		{name: "no chain segment", bucket: "stl-sentinelstaging", wantErr: true, errContains: "malformed"},
		{name: "empty", bucket: "", wantErr: true, errContains: "does not start with"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env, err := EnvironmentFromBucket(tt.bucket)
			if tt.wantErr {
				if err == nil {
					t.Errorf("EnvironmentFromBucket() expected error, got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("EnvironmentFromBucket() error = %v, want error containing %q", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("EnvironmentFromBucket() unexpected error = %v", err)
				return
			}
			if env != tt.wantEnv {
				t.Errorf("EnvironmentFromBucket() = %q, want %q", env, tt.wantEnv)
			}
		})
	}
}

// RequireChainID is now consumed by dexbootstrap + all three DEX workers
// + morpho/prime-debt/prime-allocation indexers. Per N12-7, give it the same
// table-driven coverage as the other helpers in this package.
func TestRequireChainID(t *testing.T) {
	tests := []struct {
		name        string
		envValue    string // empty = unset
		wantID      int
		wantErr     bool
		errContains string
	}{
		{
			name:     "ethereum mainnet",
			envValue: "1",
			wantID:   1,
		},
		{
			name:     "avalanche c-chain",
			envValue: "43114",
			wantID:   43114,
		},
		{
			name:        "unset",
			envValue:    "",
			wantErr:     true,
			errContains: "CHAIN_ID",
		},
		{
			name:        "non-integer",
			envValue:    "mainnet",
			wantErr:     true,
			errContains: "CHAIN_ID must be a valid integer",
		},
		{
			name:        "float-shaped",
			envValue:    "1.0",
			wantErr:     true,
			errContains: "CHAIN_ID must be a valid integer",
		},
		{
			name:        "whitespace",
			envValue:    "   ",
			wantErr:     true,
			errContains: "CHAIN_ID must be a valid integer",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("CHAIN_ID", tt.envValue)
			id, err := RequireChainID()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("RequireChainID() error = nil, want one containing %q", tt.errContains)
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("RequireChainID() error = %q, want substring %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("RequireChainID() unexpected error = %v", err)
			}
			if id != tt.wantID {
				t.Errorf("RequireChainID() = %d, want %d", id, tt.wantID)
			}
		})
	}
}
