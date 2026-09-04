package chainutil

import (
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
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

// Every chain the repo watches needs a declared block-data shape: a tool that
// re-publishes a block reads this to decide what to fetch, and refuses to start
// on a chain the map does not answer for.
func TestDefaultChainExpectations_CoversEveryKnownChain(t *testing.T) {
	expectations := DefaultChainExpectations()

	for chainID, name := range entity.ChainIDToS3Bucket {
		if _, ok := expectations[chainID]; !ok {
			t.Errorf("chain %d (%s) has no declared block-data expectation", chainID, name)
		}
	}
	for chainID := range expectations {
		if _, ok := entity.ChainIDToS3Bucket[chainID]; !ok {
			t.Errorf("chain %d is declared here but is not a chain the repo watches", chainID)
		}
	}
}

func TestChainSlug(t *testing.T) {
	tests := []struct {
		name            string
		chainID         int64
		want            string
		wantErrContains string
	}{
		{name: "ethereum", chainID: 1, want: "ethereum"},
		{name: "avalanche is the archive's name, not the chain's own avalanche-c", chainID: 43114, want: "avalanche"},
		{name: "base", chainID: 8453, want: "base"},
		{name: "optimism", chainID: 10, want: "optimism"},
		{name: "unichain", chainID: 130, want: "unichain"},
		{name: "arbitrum", chainID: 42161, want: "arbitrum"},
		{name: "a chain the repo does not watch", chainID: 999999, wantErrContains: "unknown chain ID 999999"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ChainSlug(tc.chainID)

			if tc.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Fatalf("error = %v, want one mentioning %q", err, tc.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("ChainSlug(%d) error = %v", tc.chainID, err)
			}
			if got != tc.want {
				t.Errorf("ChainSlug(%d) = %q, want %q", tc.chainID, got, tc.want)
			}
		})
	}
}
